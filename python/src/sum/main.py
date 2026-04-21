import os
import logging
import threading

from common import middleware, message_protocol, fruit_item
from common.middleware import (
    MessageMiddlewareQueueRabbitMQ,
    MessageMiddlewareExchangeRabbitMQ,
    FanoutExchangeRabbitMQ,
    DirectExchangeRabbitMQ
)

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
SUM_CONTROL_QUEUE = "SUM_CONTROL_QUEUE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_EXCHANGE = "AGGREGATION_EXCHANGE"
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
COUNT_OF_FIELDS_IN_DATA_MESSAGE = 3

class SumFilter:
    def __init__(self):
        
        logging.info(f"Starting sum filter with ID {ID}")
        
        self.eof_received_by_client = {}
        self.amount_by_fruit_by_client = {}

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.control_exchange = middleware.FanoutExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE)
        self.control_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, f"{SUM_CONTROL_QUEUE}-{ID}")
        self.data_output_exchange = middleware.DirectExchangeRabbitMQ(MOM_HOST,AGGREGATION_EXCHANGE)

    def _process_data(self, client_id, fruit, amount):
        amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    # procesa de la exchange de control
    def _run_control_polling(self):
        logging.info(f"Sum ID: {ID} | Starting control polling thread")
        self.control_queue.start_consuming(self.process_control_message)

    def process_control_message(self, message, ack, nack):
        (client_id,) = message_protocol.internal.deserialize_control(message)
        self._process_eof(client_id)
        ack()

    def _process_eof(self, client_id):
        if self.eof_received_by_client.get(client_id, False):
            logging.info(
                f"Sum ID: {ID} | Control exchange | EOF already received from client {client_id}"
            )
            return

        logging.info(f"Sum ID: {ID} | Control exchange | EOF received from client {client_id}")
        logging.info(f"Sum ID: {ID} | Send to aggregation | Processing EOF from client {client_id}")
        self.eof_received_by_client[client_id] = True
        amount_by_fruit = self.amount_by_fruit_by_client.get(client_id, {})
        
        for final_fruit_item in amount_by_fruit.values():
            
            shared_id = abs(hash(final_fruit_item.fruit)) % AGGREGATION_AMOUNT
            routing_key = f"{AGGREGATION_PREFIX}-{shared_id}"
            
            self.data_output_exchange.send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                ),
                routing_key
            )
        if client_id in self.amount_by_fruit_by_client:
            del self.amount_by_fruit_by_client[client_id]
        logging.info(f"Sum ID: {ID} | Send to aggregation | Finished processing EOF from client {client_id}")

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
            if not self.eof_received_by_client.get(client_id, False):
                logging.info(
                    f"Sum ID: {ID} | Control exchange | Forwarding EOF from client {client_id}"
                )
                self.control_exchange.send(
                    message_protocol.internal.serialize_control(client_id)
                )

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
