from playhouse.db_url import connect
import os
import json
import redis
import peewee
import sys
import uuid
import random

from time import sleep
from telstar.producer import Producer, Message

from typing import List, Tuple, Callable


class JSONField(peewee.TextField):
    def db_value(self, value):
        return json.dumps(value)

    def python_value(self, value):
        if value is not None:
            return json.loads(value)


r = redis.Redis(host=os.environ.get("REDIS_HOST"),
                port=os.environ.get("REDIS_PORT"),
                password=os.environ.get("REDIS_PASSWORD"),
                db=int(os.environ["REDIS_DB"]))

db = connect(os.environ["DATABASE"])
db.connect()


class Events(peewee.Model):
    msg_uid = peewee.UUIDField(default=uuid.uuid4)
    topic = peewee.CharField()
    data = JSONField()

    class Meta:
        database = db


if __name__ == "__main__":

    if len(sys.argv) > 1:
        if sys.argv[1] == "setup":
            print("Recreating table in order to start from scratch")
            db.drop_tables([Events])
            db.create_tables([Events])

        if "create" in sys.argv[1:]:
            for i in range(int(os.environ["RANGE_FROM"]), int(os.environ["RANGE_TO"])):
                Events.create(topic="test", data=dict(value=i))

    def puller() -> Tuple[List[Message], Callable[[], None]]:
        qs = Events.select().order_by(peewee.fn.RAND()).limit(5)
        msgs = [Message(e.topic, e.msg_uid, e.data) for e in qs]
        
        def done():
            if not os.environ.get("KEEPEVENTS"):
                for r in qs:
                    r.delete_instance()
            sleep(random.randrange(int(os.environ.get("SLEEPINESS"))) / 10)
        return msgs, done
    print("starting")
    Producer(r, puller, context_callable=db.atomic).run()
