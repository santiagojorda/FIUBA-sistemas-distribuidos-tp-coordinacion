import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
AGGREGATION_CONTROL_EXCHANGE = f"{AGGREGATION_PREFIX}_CONTROL_EXCHANGE"
AGGREGATION_CONTROL_QUEUE = f"{AGGREGATION_PREFIX}_CONTROL_QUEUE"
AGGREGATION_EXCHANGE = f"{AGGREGATION_PREFIX}_EXCHANGE"
JOIN_EXCHANGE = "JOIN_EXCHANGE"
TOP_SIZE = int(os.environ["TOP_SIZE"])

AMOUNT_FIELDS_DATA = 3

class AggregationFilter:

    def __init__(self):
        logging.info(f"Aggregation ID: {ID} | Starting aggregation filter")

        self.eof_received_by_client = {}
        self.fruit_top_by_client = {}
        self.input_queue = middleware.DirectQueueRabbitMQ(MOM_HOST, f"{AGGREGATION_PREFIX}-{ID}", AGGREGATION_EXCHANGE)
        self.output_exchange = middleware.DirectExchangeRabbitMQ(MOM_HOST, JOIN_EXCHANGE)

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Aggregation ID: {ID} | Processing data message | client: {client_id} | fruit: {fruit} | amount: {amount}")
        if client_id not in self.fruit_top_by_client:
            self.fruit_top_by_client[client_id] = []
            
        client_top = self.fruit_top_by_client[client_id]

        for i in range(len(client_top)):
            if client_top[i].fruit == fruit:
                client_top[i] = client_top[i] + fruit_item.FruitItem(fruit, amount)
                return
                
        bisect.insort(client_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        self.eof_received_by_client[client_id] = self.eof_received_by_client.get(client_id, 0) + 1
        current_eofs = self.eof_received_by_client[client_id]
        
        logging.info(f"Aggregation ID: {ID} | client: {client_id} | Received EOF ({current_eofs}/{SUM_AMOUNT})")

        if current_eofs == SUM_AMOUNT:
            logging.info(f"Aggregation ID: {ID} | client: {client_id} | All EOFs received. Calculating Top.")
            
            client_top = self.fruit_top_by_client.get(client_id, [])
            
            fruit_chunk = list(client_top[-TOP_SIZE:])
            fruit_chunk.reverse()
            
            fruit_top_serialized = list(
                map(
                    lambda item: (item.fruit, item.amount),
                    fruit_chunk,
                )
            )
            
            logging.info(f"Aggregation ID: {ID} | client: {client_id} | FINAL TOP: {fruit_top_serialized}")
            
            logging.info(f"Aggregation ID: {ID} | client: {client_id} | Sending top to join")
            self.output_exchange.send(message_protocol.internal.serialize(fruit_top_serialized))
            
            self.eof_received_by_client.pop(client_id, None)
            self.fruit_top_by_client.pop(client_id, None)
            logging.info(f"Aggregation ID: {ID} | client: {client_id} | Cleaned up internal state for client")

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if isinstance(fields, list) and len(fields) == AMOUNT_FIELDS_DATA:
            client_id, fruit, amount = fields
            logging.info(f"Aggregation ID: {ID} | client: {client_id} | Processing data message")
            self._process_data(client_id, fruit, amount)
        else:
            (client_id,) = message_protocol.internal.deserialize_control(message)
            logging.info(f"Aggregation ID: {ID} | client: {client_id} | Processing EOF message")
            self._process_eof(client_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0

if __name__ == "__main__":
    main()
