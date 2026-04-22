import os
import logging
import threading
from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
SUM_CONTROL_QUEUE = "SUM_CONTROL_QUEUE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
AGGREGATION_EXCHANGE = f"{AGGREGATION_PREFIX}_EXCHANGE"
COUNT_OF_FIELDS_IN_DATA_MESSAGE = 3

class SumFilter:
    def __init__(self):
        
        logging.info(f"Starting sum filter with ID {ID}")
        
        self.eof_received_by_client = {}
        self.condition = threading.Condition()

        self.amount_by_fruit_by_client = {}
        # self.lock = threading.Lock()

        self.input_queue = middleware.DirectQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.control_exchange = middleware.FanoutExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE)
        self.control_queue = middleware.FanoutQueueRabbitMQ(MOM_HOST, f"{SUM_CONTROL_QUEUE}-{ID}", SUM_CONTROL_EXCHANGE)
        self.data_output_exchange = middleware.DirectExchangeRabbitMQ(MOM_HOST,AGGREGATION_EXCHANGE)

    # procesa de la exchange de control
    def _run_control_polling(self):
        logging.info(f"Sum ID: {ID} | Starting control polling thread")
        self.control_queue.start_consuming(self.process_control_message)

    def process_control_message(self, message, ack, nack):
        (client_id,) = message_protocol.internal.deserialize_control(message)
        self._process_control_eof(client_id)
        ack()

    def get_aggregator_shard(self, fruit_name):
      if not fruit_name:
              return 0

      first_letter = fruit_name.lower()[0]
      letter_index = ord(first_letter) - ord('a')
      if not (0 <= letter_index <= 25):
          return 0
      return letter_index % AGGREGATION_AMOUNT

    def _process_control_eof(self, client_id):
        with self.condition:
            self.eof_received_by_client[client_id] = True
            logging.info(f"Sum ID: {ID} | client: {client_id} | Waiting residual data from message queue...")
            self.condition.wait(timeout=5)

            amount_by_fruit = self.amount_by_fruit_by_client.get(client_id, {})
            self.eof_received_by_client.pop(client_id, None)

        logging.info(f"Sum ID: {ID} | client: {client_id} | Forwarding data to aggregators. Fruits")
        for final_fruit_item in amount_by_fruit.values():
            shard_id = self.get_aggregator_shard(final_fruit_item.fruit)
            routing_key = f"{AGGREGATION_PREFIX}-{shard_id}"
            self.data_output_exchange.send(
                message_protocol.internal.serialize([client_id, final_fruit_item.fruit, final_fruit_item.amount]),
                routing_key
            )

        for aggregation_id in range(AGGREGATION_AMOUNT):
            routing_key = f"{AGGREGATION_PREFIX}-{aggregation_id}"
            self.data_output_exchange.send(message_protocol.internal.serialize_control(client_id), routing_key)
        logging.info(f"Sum ID: {ID} | client: {client_id} | Cleaned up internal state for client.")
        self.amount_by_fruit_by_client.pop(client_id, None)
        
    # procesa de la queue de ingreso  
    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == COUNT_OF_FIELDS_IN_DATA_MESSAGE:
            with self.condition:
                client_id, fruit, amount = fields
                logging.info(f"Sum ID: {ID} | Message received | client: {client_id} | fruit: {fruit} | amount: {amount}")
                is_eof = self._process_data(*fields)
                if is_eof:
                    logging.info(f"Sum ID: {ID} | client: {client_id} | Condition EOF received in data message. Forwarding to control exchange.")
                    self.condition.notify_all()
        else:
            self._send_eof_to_control_exchange(fields[0])

        ack()

    def _send_eof_to_control_exchange(self, client_id):
          client_id = int(client_id)
          logging.info(f"Sum ID: {ID} | Message received | client: {client_id} | EOF")
          self.control_exchange.send(message_protocol.internal.serialize_control(client_id))
          logging.info(f"Sum ID: {ID} | Control exchange | client: {client_id} | Forwarding EOF")

    def _process_data(self, client_id, fruit, amount):
            amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
        
            if self.eof_received_by_client.get(client_id, False):
                logging.error(f"Habia una fruta residual en la cola de mensajes para el cliente {client_id} despues de recibir EOF. Ignorando mensaje.")
                return True
            return False

    def start(self):
        control_thread = threading.Thread(
            target=self._run_control_polling,
            name=f"sum-control-polling-{ID}",
            daemon=True,
        )
        control_thread.start()

        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
