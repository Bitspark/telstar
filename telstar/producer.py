import json
import logging
from time import sleep
from typing import Callable, List, Tuple

from .com import Message, StagedMessage

log = logging.getLogger(__name__)


class Producer(object):
    def __init__(self, link, get_records: Callable[[], Tuple[List[Message], Callable[[], None]]], context_callable=None):
        self.link = link
        self.get_records = get_records
        self.context_callable = context_callable

    def run_once(self):
        records, done = self.get_records()
        pipe = self.link.pipeline()
        for msg in records:
            # Why the sleep here? It helps with sorting the events on the receiving side.
            # But it also limits to amount of possible sends to under 1k messages per send.
            # Which for now seems acceptable.
            sleep(.001)
            pipe.xadd(f"telstar:stream:{msg.stream}", {
                      Message.IDFieldName: str(msg.msg_uuid),
                      Message.DataFieldName: json.dumps(msg.data)})
        pipe.execute()
        done()

    def run(self):
        log.info("Starting main producer loop")
        while True:
            if callable(self.context_callable):
                with self.context_callable():
                    self.run_once()
            else:
                self.run_once()


class StagedProducer(Producer):
    def __init__(self, link, database, batch_size=5, wait=0.5):
        self.batch_size = batch_size
        self.wait = wait
        StagedMessage.bind(database)

        super().__init__(link, self.create_puller(), StagedMessage._meta.database.atomic)

    def create_puller(self):
        def puller() -> Tuple[List[Message], Callable[[], None]]:
            qs = StagedMessage.unsent().limit(self.batch_size).order_by(StagedMessage.id)
            msgs = [e.to_msg() for e in qs]
            log.debug(f"Found {len(msgs)} messages to be send")

            def done():
                ids = list(map(lambda l: l.id, qs))
                if ids:
                    log.debug(f"Attempting to mark {len(ids)} messages as being sent")
                    result = StagedMessage.update(sent=True).where(StagedMessage.id << ids).execute()
                    log.debug(f"Result was: {result}")
                sleep(self.wait)

            return msgs, done
        return puller
