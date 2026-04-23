import json

class WorkerControlMessageType:
    RECORD_COUNT_REQUEST = 1
    RECORD_COUNT_RESPONSE = 2
    FLUSH_REQUEST = 3
    FLUSH_SUCCESS = 4


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
