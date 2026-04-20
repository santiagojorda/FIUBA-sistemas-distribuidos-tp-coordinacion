from common import message_protocol

class MessageHandler:
    _next_id = 0

    def __init__(self):
        self.client_id = MessageHandler._next_id
        MessageHandler._next_id += 1

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields
