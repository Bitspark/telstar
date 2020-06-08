# from __future__ import annotations
import json
import uuid
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, List, Union

import peewee
from peewee import ModelSelect

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
    send_at = peewee.TimestampField(resolution=10**3)
    created_at = peewee.TimestampField(resolution=10**3)

    @classmethod
    def create(cls, **kwargs):
        if "delay" in kwargs:
            delay = kwargs.pop("delay")
            kwargs["send_at"] = datetime.now() + timedelta(seconds=delay)
        return super().create(**kwargs)


    @classmethod
    def unsent(cls) -> ModelSelect:
        return cls.select().where(cls.sent == False, cls.send_at <= datetime.now())  # noqa

    @classmethod
    def mark_as_sent(cls, messages: List["Message"]):
        ids = list(map(lambda m: m.id, messages))
        cls.update(sent=True).where(cls.id << ids).execute()

    @classmethod
    def get_transaction_wrapper(cls):
        return cls._meta.database.atomic

    @classmethod
    def setup(cls, database):
        return cls.bind(database)

    def to_telstar(self) -> "Message":
        from . import Message
        return Message(self.topic, self.msg_uid, self.data)
