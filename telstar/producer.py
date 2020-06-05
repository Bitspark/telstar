import json
import logging
from time import sleep
from typing import Callable, List, Optional, Tuple

from redis.client import Redis

from .com import Message
from .config import staging

log = logging.getLogger(__name__)


class Producer(object):
    def __init__(self, link: Redis, get_records: Callable[[], Tuple[List[Message], Callable[[], None]]], context_callable: Optional[Callable] = None) -> None:
        self.link = link
        self.get_records = get_records
        self.context_callable = context_callable

    def run_once(self) -> None:
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
    def __init__(self, link: Redis, database, batch_size: int = 5, wait: float = 0.5) -> None:
        self.batch_size = batch_size
        self.wait = wait
        staging.repository.setup(database)

        super().__init__(link, self.create_puller(), staging.repository.get_transaction_wrapper())

    def create_puller(self) -> Callable:
        producer = self

        def puller() -> Tuple[List[Message], Callable[[], None]]:
            unsent_messages = staging.repository.unsent()[:producer.batch_size]
            telstar_messages = [msg.to_telstar() for msg in unsent_messages]
            log.debug(f"Found {len(telstar_messages)} messages to be send")

            def done():
                if unsent_messages:
                    log.debug(f"Attempting to mark {len(unsent_messages)} messages as being sent")
                    result = staging.repository.mark_as_sent(unsent_messages)
                    log.debug(f"Result was: {result}")
                sleep(producer.wait)

            return telstar_messages, done
        return puller
