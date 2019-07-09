import json
import logging
import os
import random
import sys
import uuid
from time import sleep
from typing import Callable, List, Tuple

import peewee
import redis
from playhouse.db_url import connect

from telstar.com import Message
from telstar.producer import Producer

link = redis.from_url(os.environ["REDIS"])
db = connect(os.environ["DATABASE"])
db.connect()

logger = logging.getLogger('telstar')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class JSONField(peewee.TextField):
    def db_value(self, value):
        return json.dumps(value)

    def python_value(self, value):
        if value is not None:
            return json.loads(value)


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
                Events.create(topic=os.environ["STREAM_NAME"], data=dict(value=i))

    def puller() -> Tuple[List[Message], Callable[[], None]]:
        qs = Events.select().order_by(Events.id).limit(5)
        msgs = [Message(e.topic, e.msg_uid, e.data) for e in qs]

        def done():
            if not os.environ.get("KEEPEVENTS"):
                for r in qs:
                    r.delete_instance()
            sleep(random.randrange(int(os.environ.get("SLEEPINESS"))) / 10)
        return msgs, done
    print("starting")
    Producer(link, puller, context_callable=db.atomic).run()
