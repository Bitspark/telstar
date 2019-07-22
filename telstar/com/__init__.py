import json
import uuid

import peewee
from datetime import datetime


class TelstarEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, uuid.UUID):
            return str(o)

        return json.JSONEncoder.default(self, o)


class JSONField(peewee.TextField):
    def db_value(self, value):
        return json.dumps(value, cls=TelstarEncoder)

    def python_value(self, value):
        if value is not None:
            return json.loads(value)


class StagedMessage(peewee.Model):
    msg_uid = peewee.UUIDField(default=uuid.uuid4, index=True)
    topic = peewee.CharField(index=True)
    data = JSONField()

    sent = peewee.BooleanField(default=False, index=True)
    created_at = peewee.TimestampField(resolution=10**3)

    @classmethod
    def unsent(cls):
        return cls.select().where(cls.sent == False)  # noqa

    def to_msg(self):
        return Message(self.topic, self.msg_uid, self.data)


class Message(object):
    IDFieldName = b"message_id"
    DataFieldName = b"data"

    def __init__(self, stream: str, msg_uuid: uuid.UUID, data: dict):
        if not isinstance(msg_uuid, uuid.UUID):
            raise TypeError(f"msg_uuid needs to be uuid.UUID not {type(msg_uuid)}")
        if isinstance(stream, bytes):
            stream = stream.decode("ascii")
        self.stream = stream.replace("telstar:stream:", "")
        self.msg_uuid = msg_uuid
        self.data = data

    def __repr__(self):
        return f"<Message self.stream:{self.stream} msd_id:{self.msg_uuid} data:{self.data}>"


def increment_msg_id(id):
    # IDs are of the form "1509473251518-0" and comprise a millisecond
    # timestamp plus a sequence number to differentiate within the timestamp.
    time, sequence = id.decode("ascii").split("-")
    if not sequence:
        raise Exception("Argument error, {id} has wrong format not #-#")
    next_sequence = int(sequence) + 1

    return bytes(f"{time}-{next_sequence}", "ascii")


def decrement_msg_id(id):
    time, sequence = id.decode("ascii").split("-")
    if not sequence:
        raise Exception("Argument error, {id} has wrong format not #-#")
    sequence = int(sequence)
    time = int(time)
    if sequence == 0:
        time = time - 1
    else:
        sequence = int(sequence) - 1

    return bytes(f"{time}-{sequence}", "ascii")
