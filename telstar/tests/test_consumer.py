import logging
import os
import random
import sys
from time import sleep

import peewee
import redis
from playhouse.db_url import connect

from telstar.com import Message
from telstar.consumer import MultiConsumer

link = redis.from_url(os.environ["REDIS"])
db = connect(os.environ["DATABASE"])
db.connect()

logger = logging.getLogger('telstar')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class Test(peewee.Model):
    number = peewee.IntegerField()
    group_name = peewee.CharField()
    topic = peewee.CharField()

    class Meta:
        indexes = (
            (('number', 'group_name', 'topic'), True),
        )
        database = db


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        print("Recreating table in order to start from scratch")
        db.drop_tables([Test])
        db.create_tables([Test])

    def simple(consumer, record: Message, done):
        with db.atomic():
            Test.create(number=int(record.data["value"]), group_name=consumer.group_name, topic=record.stream)
            sleep(random.randrange(int(os.environ.get("SLEEPINESS"))) / 100)
            done()

    MultiConsumer(link=link,
                  group_name=os.environ.get("GROUP_NAME"),
                  consumer_name=os.environ.get("CONSUMER_NAME"),
                  config={
                      os.environ["STREAM_NAME"]: simple,
                      os.environ["STREAM_NAME_TWO"]: simple
                  }).run()
