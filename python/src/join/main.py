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


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_fruit_by_client_id = {}
        self.tops_received_by_client_id = {}

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) < 1:
            logging.error("Received top message with invalid format")
            nack()
            return

        client_id = fields[0]
        fruit_top = fields[1:]

        self.tops_received_by_client_id[client_id] = self.tops_received_by_client_id.get(client_id, 0) + 1

        if client_id not in self.amount_by_fruit_by_client_id:
            self.amount_by_fruit_by_client_id[client_id] = {}
        client_amount_by_fruit = self.amount_by_fruit_by_client_id[client_id]

        for fruit, amount in fruit_top:
            current_fruit_item = client_amount_by_fruit.get(fruit, fruit_item.FruitItem(fruit, 0))
            client_amount_by_fruit[fruit] = current_fruit_item + fruit_item.FruitItem(fruit, int(amount))

        if self.tops_received_by_client_id[client_id] == AGGREGATION_AMOUNT:
            logging.info(f"Received all tops for client {client_id}, sending output message")
            fruit_top = sorted(client_amount_by_fruit.values())
            fruit_top.reverse()
            fruit_top = fruit_top[:TOP_SIZE]
            serialized_fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_top,
                )
            )
            self.output_queue.send(message_protocol.internal.serialize([client_id] + serialized_fruit_top))
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
