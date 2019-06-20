"""
Telstar is a package to write producer and consumers groups against redis streams.
"""
__version__ = "0.0.3"
class Message(object):
    IDFieldName = b"message_id"
    DataFieldName = b"data"

    def __init__(self, stream, msg_uuid, data):
        self.stream = stream
        self.msg_uuid = msg_uuid
        self.data = data
