"""
Telstar is a package to write producer and consumers groups against redis streams.
"""
import inspect
import logging
from datetime import datetime
from functools import wraps
from typing import Callable, Dict, List, Union, Optional
from uuid import UUID

import redis
from marshmallow import Schema, ValidationError

from .admin import admin
from .com import Message
from .config import staging
from .consumer import MultiConsumer, ThreadedMultiConsumer

__version__ = "1.1.2"


logging.getLogger(__package__).addHandler(logging.NullHandler())
log = logging.getLogger(__package__)

admin = admin


def stage(topic: str, data: Dict[str, Union[int, str, datetime, UUID]], delay: Optional[int] = None) -> UUID:
    e = staging.repository.create(topic=topic, data=data, delay=delay or 0)
    return e.msg_uid


def staged() -> List[Message]:
    return [e.to_telstar() for e in staging.repository.unsent()]


class app:
    def __init__(self, link: redis.Redis, consumer_name: str, consumer_cls: MultiConsumer = ThreadedMultiConsumer, **kwargs) -> None:
        self.link: redis = link
        self.config: dict = {}
        self.consumer_name: str = consumer_name
        self.consumer_cls: MultiConsumer = consumer_cls
        self.kwargs = kwargs
        self.error_handlers = {}

    def _register_error_handler(self, exc_class, fn):
        self.error_handlers[exc_class] = fn

    def errorhandler(self, exc_class):
        def decorator(fn):
            self._register_error_handler(exc_class, fn)
        return decorator

    def get_consumer(self) -> MultiConsumer:
        return self.consumer_cls(self.link, self.consumer_name, self.config, error_handlers=self.error_handlers, **self.kwargs)

    def start(self):
        self.get_consumer().run()

    def run_once(self) -> None:
        self.get_consumer().run_once()

    def requires_full_message(self, fn: Callable) -> bool:
        argsspec = inspect.getfullargspec(fn)
        arg = argsspec.args[0]
        return argsspec.annotations[arg] is Message

    def consumer(self, group: str, streams: list, schema: Schema, strict=True, acknowledge_invalid=False) -> Callable:
        def decorator(fn):
            fullmessage = self.requires_full_message(fn)
            nonlocal streams
            if not isinstance(streams, list):
                streams = [streams]
            for stream in streams:
                @wraps(fn)
                def actual_consumer(consumer: MultiConsumer, msg: Message, done: callable):
                    try:
                        msg.data = schema().load(msg.data)
                        fn(msg) if fullmessage else fn(msg.data)
                        done()
                    except ValidationError as err:
                        log.error(f"Unable to validate message: {msg}", exc_info=True)
                        if acknowledge_invalid:
                            done()
                        if strict:
                            raise err

                if group in self.config:
                    self.config[group][stream] = actual_consumer
                else:
                    self.config[group] = {stream: actual_consumer}

        return decorator
