import os
import logging
import signal
import threading
import time
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

MAX_RETRY_ATTEMPTS = 2
RETRY_SLEEP_SECONDS = 5

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.control_exchanges = {}
        for i in range(SUM_AMOUNT):
            control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{i}"]
            )
            self.control_exchanges[i] = control_exchange
        self.fruit_amount_by_client_id = {}
        self.workers_finished_by_client_id = {}
        self.retries_by_client_id = {}
        self.current_records_by_client = {}
        self.total_records_by_client = {}
        # This dictionary is used to count how many total records were received across all workers for a given client.
        self.aggregate_records_by_client = {}

        self.resourceLock = threading.Lock()
        self.coordination_thread = None


    def _aggregation_id_for_fruit(self, client_id, fruit):
        key = f"{client_id}:{fruit}".encode("utf-8")
        return zlib.crc32(key) % AGGREGATION_AMOUNT


    def _send_client_data_to_aggregation(self, client_id):
        if client_id not in self.fruit_amount_by_client_id:
            logging.info(f"No data to flush for client {client_id}")
            return

        for final_fruit_item in self.fruit_amount_by_client_id[client_id].values():
            aggregation_id = self._aggregation_id_for_fruit(client_id, final_fruit_item.fruit)
            self.data_output_exchanges[aggregation_id].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )


    def _send_eof_to_aggregation(self, client_id):
        for output_exchange in self.data_output_exchanges:
            output_exchange.send(
                message_protocol.internal.serialize([client_id])
            )


    def _cleanup_client_state(self, client_id):
        self.fruit_amount_by_client_id.pop(client_id, None)
        self.workers_finished_by_client_id.pop(client_id, None)
        self.retries_by_client_id.pop(client_id, None)
        self.current_records_by_client.pop(client_id, None)
        self.total_records_by_client.pop(client_id, None)
        self.aggregate_records_by_client.pop(client_id, None)


    def _flush_client_data(self, coordinator_id, client_id, is_coordinator=False):
        logging.info(f"Flushing data to aggregation for client {client_id}")
        with self.resourceLock:
            self._send_client_data_to_aggregation(client_id)
            self._send_eof_to_aggregation(client_id)
            self._cleanup_client_state(client_id)
            if not is_coordinator:
                logging.info(f"Finished flushing data for client {client_id}, ACKing coordinator")
                self.control_exchanges[coordinator_id].send(message_protocol.internal.serialize([
                    message_protocol.internal.WorkerControlMessageType.FLUSH_SUCCESS,
                    client_id
                ]))

    
    def _broadcast_record_count_request(self, client_id):
        logging.info(f"Broadcasting RECORD_COUNT_REQUEST for client {client_id}")
        for instance_id, control_exchange in self.control_exchanges.items():
            if instance_id != ID:
                control_exchange.send(message_protocol.internal.serialize([
                    message_protocol.internal.WorkerControlMessageType.RECORD_COUNT_REQUEST,
                    ID,
                    client_id
                ]))
    
    
    """
    Data Processing methods for processing regular data and eof.
    These methods are called by the main data processing thread that consumes from the input queue.
    """

    def _process_data(self, client_id, fruit, amount):
        with self.resourceLock:
            if client_id not in self.fruit_amount_by_client_id:
                self.fruit_amount_by_client_id[client_id] = {}
            # logging.info(f"Processing data message for client {client_id}: worker {ID}, fruit {fruit}, amount {amount}")
            self.fruit_amount_by_client_id[client_id][fruit] = self.fruit_amount_by_client_id[client_id].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            self.current_records_by_client[client_id] = self.current_records_by_client.get(client_id, 0) + 1

    def _process_eof(self, client_id, amount_of_records):
        logging.info(f"EOF Received for client {client_id}")
        with self.resourceLock:
            self.aggregate_records_by_client[client_id] = self.aggregate_records_by_client.get(client_id, 0) + self.current_records_by_client.get(client_id, 0)
            self.total_records_by_client[client_id] = self.total_records_by_client.get(client_id, 0) + amount_of_records
            logging.info(f"Worker {ID} has sent all data for client {client_id}. Total records sent by this client: {amount_of_records}")
            if SUM_AMOUNT == 1:
                logging.info(f"Only one Sum worker, flushing data to aggregation for client {client_id}")
                self._send_client_data_to_aggregation(client_id)
                self._send_eof_to_aggregation(client_id)
                self._cleanup_client_state(client_id)
                return
            # The Sum worker that receives the EOF message becomes the Coordinator.
            logging.info(f"Broadcasting sum control message to other workers")
            self._broadcast_record_count_request(client_id)
    
    
    """
    Worker coordination methods for processing control messages related to flushing data and worker synchronization.
    These methods are called by the coordination thread that consumes from the control exchange.
    """

    def _process_count_request(self, coordinator_id, client_id):
        logging.info(f"RECORD_COUNT_REQUEST received for client {client_id}")
        with self.resourceLock:
            record_count = self.current_records_by_client.get(client_id, 0)
            self.control_exchanges[coordinator_id].send(message_protocol.internal.serialize([
                message_protocol.internal.WorkerControlMessageType.RECORD_COUNT_RESPONSE,
                client_id,
                record_count
            ]))
    
    def _process_count_response(self, client_id, record_count):
        logging.info(f"RECORD_COUNT_RESPONSE received for client {client_id}, record count {record_count}")
        with self.resourceLock:
            self.workers_finished_by_client_id[client_id] = self.workers_finished_by_client_id.get(client_id, 0) + 1
            self.aggregate_records_by_client[client_id] = self.aggregate_records_by_client.get(client_id, 0) + record_count
            if self.aggregate_records_by_client[client_id] < self.total_records_by_client[client_id]:
                logging.info(f"Waiting for more responses for client {client_id}. Total: {self.aggregate_records_by_client[client_id]}, expected: {self.total_records_by_client[client_id]}")
                if self.workers_finished_by_client_id[client_id] == SUM_AMOUNT - 1:
                    if self.retries_by_client_id[client_id] == MAX_RETRY_ATTEMPTS:
                        logging.error(f"Exceeded retry attempts for client {client_id}, expected {self.total_records_by_client[client_id]} records but got {self.aggregate_records_by_client[client_id]}. Flushing data to aggregation anyway.")
                        for instance_id, control_exchange in self.control_exchanges.items():
                            if instance_id != ID:
                                control_exchange.send(message_protocol.internal.serialize([
                                    message_protocol.internal.WorkerControlMessageType.FLUSH_REQUEST,
                                    ID,
                                    client_id
                                ]))
                        # Also flush data for this coordinator since it is responsible for sending the EOF to aggregation.
                        self._send_client_data_to_aggregation(client_id)
                        self._send_eof_to_aggregation(client_id)
                        self._cleanup_client_state(client_id)
                        return
                    logging.info(f"Not enough records received for client {client_id}. RETRYING sum control message to other workers")
                    self.workers_finished_by_client_id[client_id] = 0
                    self.aggregate_records_by_client[client_id] = self.current_records_by_client.get(client_id, 0)
                    time.sleep(RETRY_SLEEP_SECONDS)
                    self._broadcast_record_count_request(client_id)
                    self.retries_by_client_id[client_id] = self.retries_by_client_id.get(client_id, 0) + 1

            else:
                logging.info(f"Received all count responses for client {client_id}, broadcasting flush request")
                for instance_id, control_exchange in self.control_exchanges.items():
                    if instance_id != ID:
                        control_exchange.send(message_protocol.internal.serialize([
                            message_protocol.internal.WorkerControlMessageType.FLUSH_REQUEST,
                            ID,
                            client_id
                        ]))
                # Also flush data for this coordinator since it is responsible for sending the EOF to aggregation.
                self._send_client_data_to_aggregation(client_id)
                self._send_eof_to_aggregation(client_id)
                self._cleanup_client_state(client_id)

    
    """
    Middleware callback methods for consuming messages from the input queue and control exchange.
    """

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 2:
            self._process_eof(*fields)
        else:
            logging.error("Received message with invalid format")
            nack()
            return # Throw an exception here?
        ack()
    
    def process_sum_sync(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        logging.info(f"Processing sum control message: {fields}")
        if fields[0] == message_protocol.internal.WorkerControlMessageType.RECORD_COUNT_REQUEST:
            self._process_count_request(*fields[1:])
        elif fields[0] == message_protocol.internal.WorkerControlMessageType.RECORD_COUNT_RESPONSE:
            self._process_count_response(*fields[1:])
        elif fields[0] == message_protocol.internal.WorkerControlMessageType.FLUSH_REQUEST:
            self._flush_client_data(*fields[1:])
        elif fields[0] == message_protocol.internal.WorkerControlMessageType.FLUSH_SUCCESS:
            logging.info(f"FLUSH_SUCCESS message received for client {fields[1]}")
        else:
            logging.error("Received sum control message with unknown type")
            nack()
            return # Exception here as well?
        ack()

    def start(self):
        self.coordination_thread = threading.Thread(target=self.control_exchanges[ID].start_consuming, args=(self.process_sum_sync,)).start()
        self.input_queue.start_consuming(self.process_data_messsage)
    
    def stop(self):
        logging.info("Stopping SumFilter")
        self.input_queue.stop_consuming()
        for control_exchange in self.control_exchanges.values():
            control_exchange.stop_consuming()
        if self.coordination_thread:
            self.coordination_thread.join()
        logging.info("SumFilter stopped")

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: sum_filter.stop(),
    )
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
