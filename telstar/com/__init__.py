import json
import uuid

import peewee


def datetime_json(o):
    return o.isoformat() if hasattr(o, 'isoformat') else o


class JSONField(peewee.TextField):
    def db_value(self, value):
        return json.dumps(value, default=datetime_json)

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
        self.stream = stream
        self.msg_uuid = msg_uuid
        self.data = data
