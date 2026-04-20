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
COUNT_OF_FIELDS_IN_DATA_MESSAGE = 3

class SumFilter:
    def __init__(self):
        
        logging.info(f"Starting sum filter with ID {ID}")
        
        self.eof_received_by_client = {}
        self.amount_by_fruit = {}

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        # self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST)
        #self.control_queue = middleware.MessageMiddlewareSumWorkerControlQueue(MOM_HOST, ID)

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

    def _process_data(self, client_id, fruit, amount):
        self.amount_by_fruit[fruit] = self.amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))


    # procesa de la exchange de control
    def _run_control_polling(self):
        logging.info(f"Sum ID: {ID} | Starting control polling thread")
        # self.control_exchange.start_consuming(self.process_control_message)
        pass

    def process_control_message(self, message, ack, nack):
        (client_id,) = message_protocol.internal.deserialize_control(message)
        self._process_eof(client_id)
        ack()

    def _process_eof(self, client_id):
        if self.eof_received_by_client.get(client_id, False):
            logging.info(f"EOF received from client {client_id}")
            return

        self.eof_received_by_client[client_id] = True
        
        for final_fruit_item in self.amount_by_fruit.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

    # procesa de la queue de ingreso  
    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == COUNT_OF_FIELDS_IN_DATA_MESSAGE:
            client_id, fruit, amount = fields
            logging.info(
                f"Sum ID: {ID} | Message received | Data from client_id: {client_id} | fruit: {fruit} | amount: {amount}"
            )
            self._process_data(*fields)
        else:
            client_id = int(fields[0])
            logging.info(
                f"Sum ID: {ID} | Message received | EOF from client_id: {client_id} | "
            )
            self.eof_received_by_client[client_id] = True
            # self.control_exchange.send(message_protocol.internal.serialize_control(client_id))
        ack()

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
