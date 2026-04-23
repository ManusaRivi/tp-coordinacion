from common import message_protocol
import uuid

class MessageHandler:

    def __init__(self):
        # Create UUID for this instance of MessageHandler
        # This UUID uniquely identifies a connection with a single client.
        self.uuid = uuid.uuid4()
        self.amount_of_records = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.amount_of_records += 1
        return message_protocol.internal.serialize([self.uuid.hex, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.uuid.hex, self.amount_of_records])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] != self.uuid.hex:
            return None
        return fields[1:]
