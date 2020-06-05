# from __future__ import annotations
import json
import uuid
import peewee
from peewee import ModelSelect

from typing import Dict, Union, TYPE_CHECKING, List

if TYPE_CHECKING:
    from . import Message


class JSONField(peewee.TextField):
    def db_value(self, value: Dict[str, Union[int, str]]) -> str:
        from . import TelstarEncoder
        return json.dumps(value, cls=TelstarEncoder)

    def python_value(self, value: str) -> Dict[str, Union[int, str]]:
        if value is not None:
            return json.loads(value)


class StagedMessage(peewee.Model):
    msg_uid = peewee.UUIDField(default=uuid.uuid4, index=True)
    topic = peewee.CharField(index=True)
    data = JSONField()

    sent = peewee.BooleanField(default=False, index=True)
    created_at = peewee.TimestampField(resolution=10**3)

    @classmethod
    def unsent(cls) -> ModelSelect:
        return cls.select().where(cls.sent == False)  # noqa

    @classmethod
    def mark_as_sent(cls, messages: List["Message"]):
        ids = list(map(lambda m: m.id, messages))
        cls.update(sent=True).where(StagedMessage.id << ids).execute()

    def to_telstar(self) -> "Message":
        from . import Message
        return Message(self.topic, self.msg_uid, self.data)
