import logging
import os
import random
import sys
from time import sleep

import pymysql
import redis
from sqlalchemy import BIGINT, Column, String, UniqueConstraint, create_engine
from sqlalchemy.orm import sessionmaker

from telstar import config as tlconfig
from telstar.com import Message
from telstar.com.sqla import Base, StagedMessageRepository
from telstar.consumer import MultiConsumer

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


class Test(Base):
    __tablename__ = "test"
    id = Column(BIGINT(), primary_key=True)
    number = Column(BIGINT)
    group_name = Column(String(length=80))
    topic = Column(String(length=80))

    __table_args__ = (UniqueConstraint('number', 'group_name', 'topic', name='all_together'),)


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        print("Recreating table in order to start from scratch")
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    def simple(consumer, record: Message, done):
        with session.begin():
            t = Test(number=int(record.data["value"]), group_name=consumer.group_name, topic=record.stream)
            session.add(t)
            sleep(random.randrange(int(os.environ.get("SLEEPINESS"))) / 100)
            done()

    MultiConsumer(link=link,
                  group_name=os.environ.get("GROUP_NAME"),
                  consumer_name=os.environ.get("CONSUMER_NAME"),
                  config={
                      os.environ["STREAM_NAME"]: simple,
                      os.environ["STREAM_NAME_TWO"]: simple
                  }).run()
