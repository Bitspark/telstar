import uuid
from unittest import mock

import pytest
import redis
import telstar

from playhouse.db_url import connect
from telstar import Message
from telstar.consumer import Consumer
from telstar.com import StagedEvent
from telstar.producer import StagedProducer


@pytest.fixture
def link():
    return mock.MagicMock(spec=redis.Redis)

@pytest.fixture
def consumer(link):
    return Consumer(link, "group", "name", "stream", lambda msg, done: done())

@pytest.fixture
def db():
    db = connect("sqlite:///:memory:")
    db.bind([StagedEvent])
    db.create_tables([StagedEvent])
    return db

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
        Message("topic", uid , dict())

def test_checkpoint_key(consumer: Consumer):
    assert consumer._checkpoint_key() == "telstar:checkpoint:cg:telstar:stream:stream:group:name"

def test_staged_event(db):
    telstar.stage("mytopic", dict(a=1))
    assert len(StagedEvent.select().where(StagedEvent.topic == "mytopic")) == 1

def test_staged_producer(db, link):
    telstar.stage("mytopic", dict(a=1))
    [msgs], _ = StagedProducer(link, db).get_records()
    assert msgs.stream == "mytopic"
    assert msgs.data == dict(a=1)

def test_staged_producer_done_callback_removes_staged_events(db, link):
    telstar.stage("mytopic", dict(a=1))
    msgs, cb = StagedProducer(link, db).get_records()
    assert len(msgs) == 1
    assert len(telstar.staged()) == 1
    cb()
    msgs, _ = StagedProducer(link, db).get_records()
    assert len(msgs) == 0
    assert len(telstar.staged()) == 0
