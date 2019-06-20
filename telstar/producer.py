from typing import Callable, List, Tuple
import json

from . import Message


class Producer(object):
    def __init__(self, link, puller_fn: Callable[[], Tuple[List[Message], Callable[[], None]]], context_callable):
        self.link = link
        self.puller_fn = puller_fn
        self.context_callable = context_callable

    def run_once(self):
        records, done = self.puller_fn()
        for record in records:
            self.add(record)
        done()

    def run(self):
        while True:
            if callable(self.context_callable):
                with self.context_callable():
                    self.run_once()
            else:
                self.run_once()

    def add(self, msg: Message):
        self.link.xadd(f"telstar:stream:{msg.stream}", {
            Message.IDFieldName: str(msg.msg_uuid),
            Message.DataFieldName: json.dumps(msg.data)})
