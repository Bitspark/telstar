from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, String, BIGINT

from functools import partial
import logging
import os
import random
import sys
import uuid
from time import sleep
from typing import Callable, List, Tuple

import redis
import pymysql

from telstar.com import Message
from telstar.producer import Producer
from telstar import config as tlconfig

from telstar.com.sqla import StagedMessageRepository, Base, UUID, JsonEncodedDict

logger = logging.getLogger('telstar')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

pymysql.install_as_MySQLdb()


link = redis.from_url(os.environ["REDIS"])

engine = create_engine(os.environ["DATABASE"])

Session = sessionmaker(bind=engine)
session = Session(autocommit=True)

tlconfig.staging.repository = StagedMessageRepository
tlconfig.staging.repository.setup(session)


class Events(Base):
    __tablename__ = "events"
    id = Column(BIGINT(), primary_key=True)
    msg_uid = Column(UUID(), index=True, nullable=False, default=lambda: uuid.uuid4())
    topic = Column(String(length=255), index=True, nullable=False)
    data = Column(JsonEncodedDict, nullable=False)


if __name__ == "__main__":

    if len(sys.argv) > 1:
        if sys.argv[1] == "setup":
            print("Recreating table in order to start from scratch")
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)

        if "create" in sys.argv[1:]:
            with session.begin():
                for i in range(int(os.environ["RANGE_FROM"]), int(os.environ["RANGE_TO"])):
                    e = Events(topic=os.environ["STREAM_NAME"], data=dict(value=i))
                    session.add(e)

    def puller() -> Tuple[List[Message], Callable[[], None]]:
        qs = session.query(Events).order_by(Events.id).limit(5)
        msgs = [Message(e.topic, e.msg_uid, e.data) for e in qs]

        def done():
            if not os.environ.get("KEEPEVENTS"):
                for r in qs:
                    session.delete(r)
            sleep(random.randrange(int(os.environ.get("SLEEPINESS"))) / 10)
        return msgs, done
    print("starting")
    Producer(link, puller, context_callable=session.begin).run()
