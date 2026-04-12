import json

class SumControlMessageType:
    FLUSH_REQUEST = 1
    FLUSH_SUCCESS = 2


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
