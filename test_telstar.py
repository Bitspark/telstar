import uuid
from datetime import datetime
from unittest import mock

import pytest
import redis
from playhouse.db_url import connect

import telstar
from telstar.com import Message, StagedMessage
from telstar.consumer import Consumer, MultiConsumer
from telstar.producer import StagedProducer


@pytest.fixture
def link() -> redis.Redis:
    return mock.MagicMock(spec=redis.Redis)


@pytest.fixture
def consumer(link):
    return Consumer(link, "mygroup", "myname", "mytopic", lambda msg, done: done())


@pytest.fixture
def db():
    db = connect("sqlite:///:memory:")
    db.bind([StagedMessage])
    db.create_tables([StagedMessage])
    return db


def test_message():
    uid = uuid.uuid4()
    m = Message("topic", uid, dict())
    assert m.msg_uuid == uid


def test_consumer_create_group(link):
    Consumer(link, "mygroup", "myname", "mytopic", lambda msg, done: done())
    link.xgroup_create.assert_called_once_with('telstar:stream:mytopic', 'mygroup', id='0', mkstream=True)


def test_consumer_run(link: redis.Redis):
    callback = mock.Mock()
    msg_id = str(uuid.uuid4()).encode("ascii")
    link.get.return_value = None
    link.xreadgroup.return_value = [[
        b"telstar:stream:mytopic", [["stream_msg_id", {b'message_id': msg_id, b"data": "{}"}]]
    ]]
    c = Consumer(link, "mygroup", "myname", "mytopic", callback)
    c.transfer_and_process_stream_history = lambda *a, **kw: None
    c._once()
    callback.assert_called()


def test_consumer_run_callback(link: redis.Redis):
    called = False
    msg_id: bytes = str(uuid.uuid4()).encode("ascii")

    def callback(c, msg: Message, done):
        nonlocal called, msg_id
        called = True
        assert msg.msg_uuid == uuid.UUID(msg_id.decode("ascii"))
        assert msg.data == {}
        assert msg.stream == "mytopic"
        done()

    link.get.return_value = None
    link.xreadgroup.return_value = [[
        b"telstar:stream:mytopic", [["stream_msg_id", {b'message_id': msg_id, b"data": "{}"}]]
    ]]
    c = Consumer(link, "mygroup", "myname", "mytopic", callback)
    c.transfer_and_process_stream_history = lambda *a, **kw: None
    c._once()

    assert called is True


def test_consumer_checkpoint(link: redis.Redis):
    msg_id: bytes = b"49ccc393-1594-4d06-b357-0e1f322300b2"

    def callback(c, msg: Message, done):
        done()

    link.get.return_value = None
    pipeline = mock.MagicMock(spec=redis.client.Pipeline)()
    link.pipeline.return_value = pipeline
    link.xreadgroup.return_value = [[
        b"telstar:stream:mytopic", [["stream_msg_id", {b'message_id': msg_id, b"data": "{}"}]]
    ]]
    c = Consumer(link, "mygroup", "myname", "mytopic", callback)
    c._once()
    link.get.assert_any_call("telstar:checkpoint:telstar:stream:mytopic:cg:mygroup:myname")
    pipeline.set.assert_called_with("telstar:checkpoint:telstar:stream:mytopic:cg:mygroup:myname", "stream_msg_id")


def test_consumer_with_multiple_stearms(link):
    callback1 = mock.Mock()
    callback2 = mock.Mock()

    config = {
        "mytopic1": callback1,
        "mytopic2": callback2
    }
    msg_id = str(uuid.uuid4()).encode("ascii")
    link.get.return_value = None
    link.xreadgroup.return_value = [
        [b"telstar:stream:mytopic1", [
            ["stream_msg_id1", {b'message_id': msg_id, b"data": "{}"}],
            ["stream_msg_id2", {b'message_id': msg_id, b"data": "{}"}]
        ]],

        [b"telstar:stream:mytopic2", [["stream_msg_id", {b'message_id': msg_id, b"data": "{}"}]]]
    ]

    mc = MultiConsumer(link, "group", "name", config)
    mc.transfer_and_process_stream_history = lambda *a, **kw: None
    mc._once()

    assert callback1.call_count == 2
    assert callback2.call_count == 1


def test_seen_key(consumer: Consumer):
    uid_hex = "752884c3f7284cf19d3b9940373685f4"
    uid = uuid.UUID(uid_hex)
    m = Message("mytopic", uid, dict())
    assert consumer._seen_key(m) == "telstar:seen:mytopic:mygroup:752884c3-f728-4cf1-9d3b-9940373685f4"


def test_seen_key_bytes(consumer: Consumer):
    uid_hex = "752884c3f7284cf19d3b9940373685f4"
    uid = uuid.UUID(uid_hex)
    m = Message(b"mytopic", uid, dict())
    assert consumer._seen_key(m) == "telstar:seen:mytopic:mygroup:752884c3-f728-4cf1-9d3b-9940373685f4"


def test_message_strip_telstar_prefix(consumer: Consumer):
    uid_hex = "752884c3f7284cf19d3b9940373685f4"
    uid = uuid.UUID(uid_hex)
    m = Message(b"telstar:stream:mytopic", uid, dict())
    assert consumer._seen_key(m) == "telstar:seen:mytopic:mygroup:752884c3-f728-4cf1-9d3b-9940373685f4"


def test_message_with_non_uuid():
    uid = "something random"
    with pytest.raises(TypeError):
        Message("mytopic", uid, dict())


def test_checkpoint_key(consumer: Consumer):
    assert consumer._checkpoint_key("mytopic") == "telstar:checkpoint:mytopic:cg:mygroup:myname"


def test_staged_event(db):
    telstar.stage("mytopic", dict(a=1))
    assert len(StagedMessage.select().where(StagedMessage.topic == "mytopic")) == 1


def test_staged_producer(db, link):
    telstar.stage("mytopic", dict(a=1))
    [msgs], _ = StagedProducer(link, db).get_records()
    assert msgs.stream == "mytopic"
    assert msgs.data == dict(a=1)


def test_encoding_raises_correct_type_error(db, link):
    now = datetime.now()
    with pytest.raises(TypeError):
        telstar.stage("mytopic", dict(dt=now, mock=mock.MagicMock()))


def test_stage_can_encode_types(db, link):
    now = datetime.now()
    uid = uuid.uuid4()
    telstar.stage("mytopic", dict(dt=now, uuid=uid))
    [msg], cb = StagedProducer(link, db).get_records()
    assert msg.data == {"dt": now.isoformat(), "uuid": str(uid)}


def test_staged_producer_done_callback_removes_staged_events(db, link):
    telstar.stage("mytopic", dict(a=1))
    msgs, cb = StagedProducer(link, db).get_records()
    assert len(msgs) == 1
    assert len(telstar.staged()) == 1
    cb()
    msgs, _ = StagedProducer(link, db).get_records()
    assert len(msgs) == 0
    assert len(telstar.staged()) == 0
