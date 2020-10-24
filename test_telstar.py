import os
import uuid
import time
from datetime import datetime
from unittest import mock

import peewee
import pytest
import redis
import pymysql
from marshmallow import Schema, ValidationError, fields
from playhouse.db_url import connect
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import StatementError

import telstar
from telstar import config as tlconfig
from telstar.com import Message, MessageError
from telstar.com.pw import StagedMessage as StagedMessagePeeWee
from telstar.com.sqla import StagedMessageRepository as StagedMessageSqlAlchemy
from telstar.consumer import Consumer, MultiConsumeOnce, MultiConsumer
from telstar.producer import StagedProducer

pymysql.install_as_MySQLdb()

def pytest_configure(config):
    config.addinivalue_line(
        "integration", "uses real redis and mysql"
    )


@pytest.fixture
def msg_schema() -> Schema:
    class MyObjSchema(Schema):
        name = fields.Str()
        email = fields.Email()
    return MyObjSchema


@pytest.fixture
def link() -> redis.Redis:
    return mock.MagicMock(spec=redis.Redis)


@pytest.fixture
def reallink() -> redis.Redis:
    client = redis.from_url(os.environ.get("REDIS", "redis://localhost:6379/10"))
    client.flushdb()
    return client


def peewee_db_setup(connection_uri):
    tables = [tlconfig.staging.repository]
    db = connect(connection_uri)
    db.bind(tables)
    db.drop_tables(tables)
    db.create_tables(tables)
    return db


def sqlalchemy_db_setup(connection_uri):
    engine = create_engine(connection_uri)
    return engine

pytest.mark.only_sqla = pytest.mark.skipif(os.environ.get("ORM") != "sqlalchemy", reason="Not supported by peewee")
pytest.mark.only_peewee = pytest.mark.skipif(os.environ.get("ORM") == "sqlalchemy", reason="Not supported by sql")

@pytest.fixture(scope="session")
def db_engine():
    connection_uri = os.environ.get("DATABASE", "postgres://127.0.0.1:5432/telstar-integration-test")
    if os.environ.get("ORM") == "peewee":
        tlconfig.staging.repository = StagedMessagePeeWee
        yield peewee_db_setup(connection_uri)

    if os.environ.get("ORM") == "sqlalchemy":
        from telstar.com.sqla import Base
        tlconfig.staging.repository = StagedMessageSqlAlchemy
        db_engine = sqlalchemy_db_setup(connection_uri)
        Base.metadata.create_all(db_engine)

        yield db_engine

        Base.metadata.drop_all(db_engine)


@pytest.fixture
def session_maker(db_engine) -> peewee.Database:
    if os.environ.get("ORM") == "peewee":
        yield db_engine

    if os.environ.get("ORM") == "sqlalchemy":
        connection = db_engine.connect()
        Session = sessionmaker(bind=db_engine)

        yield Session

        connection.close()


@pytest.fixture
def db_session(session_maker) -> peewee.Database:
    if os.environ.get("ORM") == "peewee":
        with session_maker.transaction() as txn:
            yield session_maker
            txn.rollback()

    if os.environ.get("ORM") == "sqlalchemy":
        session = session_maker()
        tlconfig.staging.repository.setup(session)

        yield session

        session.rollback()
        session.close()


@pytest.fixture
def consumer(link) -> Consumer:
    return Consumer(link, "mygroup", "myname", "mytopic", lambda msg, done: done())


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
    c.run_once()
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
    c.run_once()

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
    c.run_once()
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
    mc.run_once()

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


@pytest.mark.only_peewee
def test_staged_event(db_session):
    telstar.stage("mytopic", dict(a=1))
    assert len(tlconfig.staging.repository.select().where(tlconfig.staging.repository.topic == "mytopic")) == 1


def test_staged_producer(db_session, link):
    telstar.stage("mytopic", dict(a=1))
    [msgs], _ = StagedProducer(link, db_session).get_records()
    assert msgs.stream == "mytopic"
    assert msgs.data == dict(a=1)


def test_encoding_raises_correct_type_error(db_session, link):
    now = datetime.now()

    Error = TypeError
    if os.environ["ORM"] == "sqlalchemy":
        Error = StatementError

    with pytest.raises(Error):
        telstar.stage("mytopic", dict(dt=now, mock=mock.MagicMock()))
        db_session.commit()


def test_stage_can_encode_types(db_session, link):
    now = datetime.now()
    uid = uuid.uuid4()
    telstar.stage("mytopic", dict(dt=now, uuid=uid))
    [msg], cb = StagedProducer(link, db_session).get_records()
    assert msg.data == {"dt": now.isoformat(), "uuid": str(uid)}


def test_staged_producer_done_callback_removes_staged_events(db_session, link):
    telstar.stage("mytopic", dict(a=1))
    telstar.stage("mytopic", dict(a=1))
    msgs, cb = StagedProducer(link, db_session, batch_size=1).get_records()
    assert len(msgs) == 1
    assert len(telstar.staged()) == 2
    cb()
    msgs, _ = StagedProducer(link, db_session).get_records()
    assert len(msgs) == 1
    assert len(telstar.staged()) == 1


@pytest.mark.only_sqla
def test_staged_producer_context_callable(session_maker, link):
    session = session_maker(autocommit=True)
    tlconfig.staging.repository.setup(session)

    with session.begin():
        telstar.stage("mytopic", dict(a=1))
        telstar.stage("mytopic", dict(b=1))

    sp = StagedProducer(link, session, batch_size=10)

    with sp.context_callable():
        sp.run_once()

    with sp.context_callable():
        sp.run_once()

def test_staged_producer_delay_sending_message(db_session, link):
    telstar.stage("mytopic", dict(a=1), delay=4)
    telstar.stage("mytopic", dict(b=1))
    msgs, cb = StagedProducer(link, db_session, batch_size=10).get_records()
    assert len(msgs) == 1
    assert len(telstar.staged()) == 1
    cb()
    msgs, _ = StagedProducer(link, db_session).get_records()
    assert len(msgs) == 0
    assert len(telstar.staged()) == 0
    time.sleep(5)
    msgs, _ = StagedProducer(link, db_session).get_records()
    assert len(msgs) == 1
    assert len(telstar.staged()) == 1

def test_consumer_once_keys(link):
    callback = mock.Mock()
    m = MultiConsumeOnce(link, "testgroup", {"mystream": callback})
    assert m._applied_key() == "telstar:once:testgroup"


@pytest.mark.integration
def test_app_pattern(db_session, reallink, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    m = mock.Mock()

    @app.consumer("group", ["mytopic", "mytopic2"], schema=msg_schema)
    def callback(data: dict):
        m()

    telstar.stage("mytopic", dict(name="1", email="a@b.com"))
    telstar.stage("mytopic2", dict(name="1", email="a@b.com"))
    StagedProducer(reallink, db_session).run_once()

    app.run_once()
    assert m.call_count == 2


@pytest.mark.integration
def test_app_consumer_strictness(db_session, reallink, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")

    @app.consumer("group", "mytopic", schema=msg_schema, strict=True)
    def callback(data: dict):
        print(data)

    telstar.stage("mytopic", dict(name="1", email="invalid"))
    StagedProducer(reallink, db_session).run_once()

    with pytest.raises(ValidationError):
        app.run_once()


@pytest.mark.integration
def test_app_consumer_errorhandler(db_session, reallink, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    m = mock.Mock()

    @app.consumer("group", "mytopic", schema=msg_schema, strict=True)
    def callback(data: dict):
        print(data)

    @app.errorhandler(ValidationError)
    def handler(exc, ack, rec):
        m()

    telstar.stage("mytopic", dict(name="1", email="invalid"))
    StagedProducer(reallink, db_session).run_once()

    app.run_once()
    assert m.call_count == 1


@pytest.mark.integration
def test_app_consumer_errorhandler_can_acknowledge(db_session, reallink, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    m = mock.Mock()

    @app.consumer("group", "mytopic", schema=msg_schema, strict=True)
    def callback(data: dict):
        print(data)

    @app.errorhandler(ValidationError)
    def handler(exc, ack, rec):
        m()
        ack()

    telstar.stage("mytopic", dict(name="1", email="invalid"))
    StagedProducer(reallink, db_session).run_once()

    app.run_once()
    app.run_once()
    assert m.call_count == 1  # The message is ack'ed in the error handler

@pytest.mark.integration
def test_app_consumer_errorhandler_receives_record(db_session, reallink, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    m = mock.Mock()

    @app.consumer("group", "mytopic", schema=msg_schema, strict=True)
    def callback(data: dict):
        print(data)

    @app.errorhandler(ValidationError)
    def handler(exc, ack, record):
        m(record)
        ack()

    telstar.stage("mytopic", dict(name="1", email="invalid"))
    StagedProducer(reallink, db_session).run_once()

    app.run_once()
    assert m.call_count == 1  # The message is ack'ed in the error handler
    [msg, ] = m.call_args[0]
    assert msg[b"data"] == b'{"name": "1", "email": "invalid"}'


@pytest.mark.integration
def test_app_consumer_invalid_message(db_session, reallink: redis.Redis, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    m = mock.Mock()

    @app.consumer("group", "mytopic", schema=msg_schema, strict=True)
    def callback(data: dict):
        print(data)

    @app.errorhandler(MessageError)
    def handler(exc, ack, rec):
        m()
        ack()

    reallink.xadd("telstar:stream:mytopic", {"not_uuid": "asd", "not_data": "asd"})
    app.run_once()
    app.run_once()
    assert m.call_count == 1  # The message is ack'ed in the error handler


@pytest.mark.integration
def test_app_consumer_ack_invalid(db_session, reallink, mocker, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")

    @app.consumer("group", "mytopic", schema=msg_schema, acknowledge_invalid=True, strict=False)
    def callback(data: dict):
        pass

    telstar.stage("mytopic", dict(name="1", email="a@b.com"))
    StagedProducer(reallink, db_session).run_once()

    ack = mocker.spy(MultiConsumer, "acknowledge")
    app.run_once()
    ack.assert_called_once()


@pytest.mark.integration
def test_app_consumer_do_not_ack_invalid(db_session, reallink, mocker, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    m = mock.Mock()

    @app.consumer("group", "mytopic", schema=msg_schema, acknowledge_invalid=False, strict=False)
    def callback(data: dict):
        m()

    telstar.stage("mytopic", dict(name="1", email="invalid"))
    StagedProducer(reallink, db_session).run_once()

    ack = mocker.spy(MultiConsumer, "acknowledge")
    app.run_once()
    app.run_once()
    ack.assert_not_called()


@pytest.mark.integration
def test_app_consumer_full_message(db_session, reallink, mocker, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    m = mock.Mock()

    @app.consumer("group", "mytopic", schema=msg_schema, acknowledge_invalid=False, strict=False)
    def callback(data: Message):
        m(data)

    telstar.stage("mytopic", dict(name="1", email="a@b.com"))
    StagedProducer(reallink, db_session).run_once()

    app.run_once()
    [msg, ] = m.call_args[0]
    assert msg.data == dict(name="1", email="a@b.com")
    assert isinstance(msg, Message)


@pytest.mark.integration
def test_consumer_once(db_session, reallink):
    result = list()
    for i in range(10):
        telstar.stage("mytopic", dict(i=i))

    def callback(c, msg: Message, done):
        data = int(msg.data["i"])
        if data < c.stop:
            result.append(data)
            done()

    sp = StagedProducer(reallink, db_session, batch_size=100)
    sp.run_once()

    m = MultiConsumeOnce(reallink, "mytest", {"mytopic": callback})
    m.stop = 5
    assert m.run() == 10  # Successfully proccessed 10 message but only five got ack'ed

    m.stop = 10
    assert m.run() == 5  # Processes the remaining five
    assert m.run() == 0  # Nothing to process anylonger

    # The message should appear in the order they where sent in.
    assert result == list(range(10))


@pytest.mark.integration
def test_admin_basics(reallink, db_session, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    admin = telstar.admin(reallink)
    telstar.stage("mytopic", dict(name="1", email="a@b.com"))

    sp = StagedProducer(reallink, db_session, batch_size=100)
    sp.run_once()

    [stream] = admin.get_streams()
    # We have not yet read something from the stream thus there is now group and no consumer
    assert stream.get_pending_messages() == []
    assert stream.get_groups() == []

    @app.consumer("group", "mytopic", schema=msg_schema, acknowledge_invalid=False, strict=False)
    def callback(data: Message):
        raise Exception("Wont't process")

    with pytest.raises(Exception):
        app.run_once()
    with pytest.raises(Exception):
        app.run_once()

    [msg] = stream.get_pending_messages()
    [grp] = stream.get_groups()
    [consumer] = grp.get_consumers()

    assert stream.get_length() == 1
    assert grp.get_seen_messages() == 0
    assert stream.display_name == b"mytopic"

    assert msg.times_delivered == 2
    assert consumer.idle_time < 1000


@pytest.mark.integration
def test_admin_group_deletion(reallink, db_session, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    admin = telstar.admin(reallink)
    telstar.stage("mytopic", dict(name="1", email="a@b.com"))

    sp = StagedProducer(reallink, db_session, batch_size=100)
    sp.run_once()

    @app.consumer("group", "mytopic", schema=msg_schema)
    def callback(data: Message):
        pass

    app.run_once()

    [stream] = admin.get_streams()
    [grp] = stream.get_groups()

    grp.delete()

    assert stream.get_groups() == []


@pytest.mark.integration
def test_admin_message_removal(reallink, db_session, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    admin = telstar.admin(reallink)
    m = mock.Mock()

    telstar.stage("mytopic", dict(name="1", email="a@b.com"))


    sp = StagedProducer(reallink, db_session, batch_size=100)
    sp.run_once()

    @app.consumer("group", "mytopic", schema=msg_schema)
    def callback(data: Message):
        raise MessageError()

    @app.errorhandler(MessageError)
    def handler(exc, ack, rec):
        m()

    app.run_once()
    app.run_once()

    [stream] = admin.get_streams()
    [msg] = stream.get_pending_messages()

    assert m.call_count == 2  # The message is ack'ed in the error handler
    assert msg.times_delivered == 2

    msg.remove()
    app.run_once()

    # Message has been remove this not delivering again
    assert m.call_count != 3


@pytest.mark.integration
def test_admin_read_pending_message(reallink, db_session, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    admin = telstar.admin(reallink)
    data = dict(name="1", email="a@b.com")
    telstar.stage("mytopic", data)

    sp = StagedProducer(reallink, db_session, batch_size=100)
    sp.run_once()

    @app.consumer("group", "mytopic", schema=msg_schema, acknowledge_invalid=False, strict=False)
    def callback(data: Message):
        raise Exception("Wont't process")

    with pytest.raises(Exception):
        app.run_once()

    [stream] = admin.get_streams()
    [grp] = stream.get_groups()
    [msg] = grp.get_pending_messages()
    assert msg.read().data == data


@pytest.mark.integration
def test_admin_consumer_deletion(reallink, db_session, msg_schema):
    app = telstar.app(reallink, consumer_name="c1")
    admin = telstar.admin(reallink)
    telstar.stage("mytopic", dict(name="1", email="a@b.com"))

    sp = StagedProducer(reallink, db_session, batch_size=100)
    sp.run_once()

    @app.consumer("group", "mytopic", schema=msg_schema)
    def callback(data: Message):
        pass

    app.run_once()

    [stream] = admin.get_streams()
    [grp] = stream.get_groups()
    [consumer] = grp.get_consumers()

    consumer.delete()

    assert grp.get_consumers() == []


@pytest.mark.integration
def test_consume_order(db_session, reallink):
    result = list()
    telstar.stage("mytopic", dict(i=1))
    telstar.stage("mytopic2", dict(i=2))
    telstar.stage("mytopic2", dict(i=3))
    telstar.stage("mytopic", dict(i=4))
    telstar.stage("mytopic2", dict(i=5))
    telstar.stage("mytopic2", dict(i=6))
    telstar.stage("mytopic2", dict(i=7))
    telstar.stage("mytopic", dict(i=8))
    telstar.stage("mytopic", dict(i=9))
    telstar.stage("mytopic2", dict(i=10))
    telstar.stage("mytopic2", dict(i=11))
    telstar.stage("mytopic2", dict(i=12))

    def callback(c, msg: Message, done):
        data = int(msg.data["i"])
        result.append(data)
        done()

    sp = StagedProducer(reallink, db_session, batch_size=100)
    sp.run_once()
    m = MultiConsumeOnce(reallink, "mytest", {"mytopic": callback, "mytopic2": callback})
    m.run()

    def monotonicity(l):
        # Count the number of times the current element is one smaller than the next
        return sum([x + 1 == y for x, y in zip(l, l[1:])])

    # Maximum monotony
    assert monotonicity(result) >= 3
