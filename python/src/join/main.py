import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
JOIN_EXCHANGE = "JOIN_EXCHANGE"

class JoinFilter:

    def __init__(self):
        logging.info("Starting join filter")

        self.eof_received_by_client = {}
        self.fruit_top_by_client = {}

        self.input_queue = middleware.DirectQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE, JOIN_EXCHANGE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def process_messsage(self, message, ack, nack):
        client_id, fruit, amount = message_protocol.internal.deserialize(message)
        logging.info(f"Join | Received top fruit | client: {client_id} | fruit: {fruit} | amount: {amount}")
        fruit_top = message_protocol.internal.deserialize(message)
        self.output_queue.send(message_protocol.internal.serialize(fruit_top))
        logging.info(f"Join | Sending top")
        ack()

    def start(self):
        logging.info("Join | Starting to consume messages")
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
