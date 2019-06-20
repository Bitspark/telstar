import uuid
import pytest
import redis
from unittest import mock
from telstar import Message
from telstar.consumer import Consumer

@pytest.fixture
def consumer():
    return Consumer(mock.MagicMock(spec=redis.Redis), "group", "name", "stream", lambda msg, done: done())

def test_message():
    uid = uuid.uuid4()
    m = Message("topic", uid , dict())
    assert m.msg_uuid == uid

def test_seen_key(consumer: Consumer):
    uid_hex = "752884c3f7284cf19d3b9940373685f4"
    uid = uuid.UUID(uid_hex)
    m = Message("topic", uid , dict())
    assert consumer._seen_key(m) == "telstar:seen:telstar:stream:stream:group:752884c3-f728-4cf1-9d3b-9940373685f4"

def test_message_with_non_uuid():
    uid = "something random"
    with pytest.raises(TypeError):
        m = Message("topic", uid , dict())

def test_checkpoint_key(consumer: Consumer):
    assert consumer._checkpoint_key() == "telstar:checkpoint:cg:telstar:stream:stream:group:name"