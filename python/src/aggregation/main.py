import os
import logging
import bisect
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

        self.workers_finished_by_client_id = {}

        self.resourceLock = threading.Lock()

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        with self.resourceLock:
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
        with self.resourceLock:
            self.workers_finished_by_client_id[client_id] = 1
            for instance_id, control_exchange in self.control_exchanges.items():
                if instance_id != ID:
                    control_exchange.send(message_protocol.internal.serialize([
                        message_protocol.internal.WorkerControlMessageType.FLUSH_REQUEST,
                        ID,
                        client_id
                    ]))
    
    def _send_fruit_top(self, coordinator_id, client_id):
        logging.info(f"FLUSH_REQUEST received for client {client_id}, sending top to output_queue")
        with self.resourceLock:
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
            self.output_queue.send(message_protocol.internal.serialize([client_id] + fruit_top))
            logging.info(f"Sending FLUSH_SUCCESS message to coordinator {coordinator_id}")
            self.control_exchanges[coordinator_id].send(message_protocol.internal.serialize([
                message_protocol.internal.WorkerControlMessageType.FLUSH_SUCCESS,
                client_id
            ]))

    def _process_worker_finished(self, client_id):
        logging.info(f"FLUSH_SUCCESS message received for client {client_id}")
        with self.resourceLock:
            self.workers_finished_by_client_id[client_id] = self.workers_finished_by_client_id.get(
                client_id, 0
            ) + 1
            if self.workers_finished_by_client_id[client_id] == AGGREGATION_AMOUNT:
                logging.info(f"All other workers have sent their top for client {client_id}")
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
                self.output_queue.send(message_protocol.internal.serialize([client_id] + fruit_top))

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()
    
    def process_aggregation_sync(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        logging.info(f"Processing aggregation control message: {fields}")
        if fields[0] == message_protocol.internal.WorkerControlMessageType.FLUSH_REQUEST:
            self._send_fruit_top(*fields[1:])
        elif fields[0] == message_protocol.internal.WorkerControlMessageType.FLUSH_SUCCESS:
            self._process_worker_finished(*fields[1:])
        else:
            logging.error("Received aggregation control message with unknown type")
            nack()
            return # Exception here as well?
        ack()

    def start(self):
        threading.Thread(target=self.control_exchanges[ID].start_consuming, args=(self.process_aggregation_sync,)).start()
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
