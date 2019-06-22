"""
Telstar is a package to write producer and consumers groups against redis streams.
"""
import uuid

from .com import StagedMessage

__version__ = "0.0.9"


def stage(topic, data):
    e = StagedMessage.create(topic=topic, data=data)
    return e.msg_uid


def staged():
    return [e.to_msg() for e in StagedMessage.unsent()]


class Message(object):
    IDFieldName = b"message_id"
    DataFieldName = b"data"

    def __init__(self, stream, msg_uuid: uuid.UUID, data):
        if not isinstance(msg_uuid, uuid.UUID):
            raise TypeError(f"msg_uuid needs to be uuid.UUID not {type(msg_uuid)}")
        self.stream = stream
        self.msg_uuid = msg_uuid
        self.data = data
