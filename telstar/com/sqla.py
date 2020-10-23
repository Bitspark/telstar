import json
import uuid
from datetime import datetime, timedelta

from sqlalchemy import TIMESTAMP, BigInteger, Boolean, Column, String, Text
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.dialects.postgresql import UUID as psqlUUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import BINARY, TypeDecorator
from sqlalchemy.sql import functions as func

Base = declarative_base()

__all__ = ["StagedMessageRepository"]


BigIntegerType = BigInteger()
BigIntegerType = BigIntegerType.with_variant(postgresql.BIGINT(), 'postgresql')
BigIntegerType = BigIntegerType.with_variant(mysql.BIGINT(), 'mysql')
BigIntegerType = BigIntegerType.with_variant(sqlite.INTEGER(), 'sqlite')


class JsonEncodedDict(TypeDecorator):
    """Enables JSON storage by encoding and decoding on the fly."""
    impl = Text

    def process_bind_param(self, value, dialect):
        from . import TelstarEncoder

        if value is None:
            return '{}'
        else:
            return json.dumps(value, cls=TelstarEncoder)

    def process_result_value(self, value, dialect):
        if value is None:
            return {}
        else:
            return json.loads(value)


class UUID(TypeDecorator):
    """Platform-independent GUID type.

    Uses Postgresql's UUID type, otherwise uses
    BINARY(16), to store UUID.

    """
    impl = BINARY

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(psqlUUID())
        else:
            return dialect.type_descriptor(BINARY(16))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                if isinstance(value, bytes):
                    value = uuid.UUID(bytes=value)
                elif isinstance(value, int):
                    value = uuid.UUID(int=value)
                elif isinstance(value, str):
                    value = uuid.UUID(value)
        if dialect.name == 'postgresql':
            return str(value)
        else:
            return value.bytes

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        if dialect.name == 'postgresql':
            return uuid.UUID(value)
        else:
            return uuid.UUID(bytes=value)


class StagedMessage(Base):
    __tablename__ = 'telstar_staged_message'

    id = Column(BigIntegerType, primary_key=True)
    msg_uid = Column(UUID(), index=True, nullable=False, default=lambda: uuid.uuid4())
    topic = Column(String(length=255), index=True, nullable=False)
    data = Column(JsonEncodedDict, nullable=False)

    sent = Column(Boolean(), default=False, index=True)
    send_at = Column(TIMESTAMP())
    created_at = Column(TIMESTAMP(), server_default=func.now())

    def to_telstar(self):
        from . import Message
        return Message(self.topic, self.msg_uid, self.data)


class _StagedMessageRepository:
    def __init__(self):
        self.model: StagedMessage = StagedMessage

    def create(self, **kwargs):
        if "delay" in kwargs:
            delay = kwargs.pop("delay")
            kwargs["send_at"] = datetime.now() + timedelta(seconds=delay)
        obj = self.model(**kwargs)
        self.db.add(obj)
        return obj

    def setup(self, database):
        self.db = database

    def get_transaction_wrapper(self):
        return self.db.begin

    def unsent(self):
        # We look into past when sending
        # If you remove the timedelta we get strange errors
        return self.db.query(self.model).filter(self.model.sent == False, self.model.send_at <= datetime.now() + timedelta(seconds=1)).order_by(self.model.id)

    def mark_as_sent(self, messages):
        for m in messages:
            m.sent = True


StagedMessageRepository = _StagedMessageRepository()
