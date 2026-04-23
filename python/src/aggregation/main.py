import os
import logging
import bisect
import signal
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
AGGREGATION_CONTROL_EXCHANGE = "AGGREGATION_CONTROL_EXCHANGE"

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.control_exchanges = {}
        for i in range(AGGREGATION_AMOUNT):
            control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_CONTROL_EXCHANGE, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.control_exchanges[i] = control_exchange
        self.fruit_tops_by_client_id = {}
        self.sum_workers_finished_by_client_id = {}

    def _cleanup_client_state(self, client_id):
        self.fruit_tops_by_client_id.pop(client_id, None)
        self.sum_workers_finished_by_client_id.pop(client_id, None)

    def _send_fruit_top_for_client(self, client_id):
        logging.info(f"Sending top message")
        fruit_top = []
        if client_id in self.fruit_tops_by_client_id:
            fruit_chunk = list(self.fruit_tops_by_client_id[client_id][-TOP_SIZE:])
            fruit_chunk.reverse()
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_chunk,
                )
            )
        # Always send one message per aggregation worker so Join can count AGGREGATION_AMOUNT.
        self.output_queue.send(message_protocol.internal.serialize([client_id] + fruit_top))

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        if client_id not in self.fruit_tops_by_client_id:
            self.fruit_tops_by_client_id[client_id] = []
        # Check if the fruit's already in the list and save the index:
        # After list is iterated, element is removed and reinserted.
        # Otherwise, it's modified in the same spot and the sorting is broken.
        already_in_list = False
        previous_index = 0
        fruit_top = self.fruit_tops_by_client_id[client_id]
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                already_in_list = True
                previous_index = i
        if already_in_list:
            amount += fruit_top[previous_index].amount
            fruit_top.pop(previous_index)
        
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF, broadcasting flush request for client {client_id}")
        # Only do this if all workers have sent EOF.
        # Increment sum workers finished for that client, and if that equals SUM_AMOUNT,
        # send flush request to all other aggregation workers and send top for client to output_queue.
        self.sum_workers_finished_by_client_id[client_id] = self.sum_workers_finished_by_client_id.get(
            client_id, 0
        ) + 1
        if self.sum_workers_finished_by_client_id[client_id] < SUM_AMOUNT:
            logging.info(f"Received EOF from {self.sum_workers_finished_by_client_id[client_id]} sum workers for client {client_id}, waiting for more")
        else:
            logging.info(f"Received EOF from all sum workers for client {client_id}")
            self._send_fruit_top_for_client(client_id)
            self._cleanup_client_state(client_id)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
    
    def stop(self):
        logging.info("Stopping AggregationFilter")
        self.input_exchange.stop_consuming()
        for control_exchange in self.control_exchanges.values():
            control_exchange.stop_consuming()
        self.output_queue.close()
        logging.info("AggregationFilter stopped")


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: aggregation_filter.stop(),
    )
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
