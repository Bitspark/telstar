import json
from time import sleep
from typing import Callable, List, Tuple

from . import Message
from .com import StagedEvent


class Producer(object):
    def __init__(self, link, get_records: Callable[[], Tuple[List[Message], Callable[[], None]]], context_callable=None):
        self.link = link
        self.get_records = get_records
        self.context_callable = context_callable

    def run_once(self):
        records, done = self.get_records()
        for record in records:
            self.send(record)
        done()

    def run(self):
        while True:
            if callable(self.context_callable):
                with self.context_callable():
                    self.run_once()
            else:
                self.run_once()

    def send(self, msg: Message):
        self.link.xadd(f"telstar:stream:{msg.stream}", {
            Message.IDFieldName: str(msg.msg_uuid),
            Message.DataFieldName: json.dumps(msg.data)})


class StagedProducer(Producer):
    def __init__(self, link, database, batch_size=5, wait=0.5):
        self.batch_size = batch_size
        self.wait = wait
        StagedEvent.bind(database)

        super().__init__(link, self.create_puller(), StagedEvent._meta.database.atomic)

    def create_puller(self):
        def puller() -> Tuple[List[Message], Callable[[], None]]:
            qs = StagedEvent.unsent().limit(self.batch_size)
            msgs = [e.to_msg() for e in qs]

            def done():
                ids = list(map(lambda l: l.id, qs))
                if ids:
                    StagedEvent.update(sent=True).where(StagedEvent.id in ids).execute()
                sleep(self.wait)

            return msgs, done
        return puller
