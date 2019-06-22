import os
import random
import sys
from time import sleep

import peewee
import redis
from playhouse.db_url import connect
from telstar.com import Message
from telstar.consumer import Consumer

r = redis.Redis(host=os.environ.get("REDIS_HOST"),
                port=os.environ.get("REDIS_PORT"),
                password=os.environ.get("REDIS_PASSWORD"),
                db=int(os.environ.get("REDIS_DB")))

db = connect(os.environ["DATABASE"])
db.connect()


class Test(peewee.Model):
    number = peewee.IntegerField()
    group_name = peewee.CharField()

    class Meta:
        indexes = (
            (('number', 'group_name'), True),
        )
        database = db


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        print("Recreating table in order to start from scratch")
        db.drop_tables([Test])
        db.create_tables([Test])

    def simple(consumer, record: Message, done):
        with db.atomic():
            Test.create(number=int(record.data["value"]), group_name=consumer.group_name)
            sleep(random.randrange(int(os.environ.get("SLEEPINESS"))) / 100)
            done()

    Consumer(link=r,
             stream_name=os.environ.get("STREAM_NAME"),
             group_name=os.environ.get("GROUP_NAME"),
             consumer_name=os.environ.get("CONSUMER_NAME"),
             processor_fn=simple).run()
