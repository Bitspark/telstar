"""
Telstar is a package to write producer and consumers groups against redis streams.
"""
__version__ = "0.2.1"

import inspect
import logging
from functools import wraps

import redis
from marshmallow import Schema, ValidationError

from .com import Message, StagedMessage
from .consumer import MultiConsumer, ThreadedMultiConsumer
from .admin import admin

logging.getLogger(__package__).addHandler(logging.NullHandler())
log = logging.getLogger(__package__)

admin = admin


def stage(topic, data):
    e = StagedMessage.create(topic=topic, data=data)
    return e.msg_uid


def staged():
    return [e.to_msg() for e in StagedMessage.unsent()]


class app:
    def __init__(self, link: redis.Redis, consumer_name: str, consumer_cls: MultiConsumer = ThreadedMultiConsumer, **kwargs):
        self.link: redis = link
        self.config: dict = {}
        self.consumer_name: str = consumer_name
        self.consumer_cls: MultiConsumer = consumer_cls
        self.kwargs = kwargs

    def get_consumer(self):
        return self.consumer_cls(self.link, self.consumer_name, self.config, **self.kwargs)

    def start(self):
        self.get_consumer().run()

    def run_once(self):
        self.get_consumer().run_once()

    def requires_full_message(self, fn):
        argsspec = inspect.getfullargspec(fn)
        arg = argsspec.args[0]
        return argsspec.annotations[arg] is Message

    def consumer(self, group: str, streams: list, schema: Schema, strict=True, acknowledge_invalid=False):
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
