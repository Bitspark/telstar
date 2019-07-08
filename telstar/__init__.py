"""
Telstar is a package to write producer and consumers groups against redis streams.
"""
from .com import StagedMessage

__version__ = "0.1.1"


def stage(topic, data):
    e = StagedMessage.create(topic=topic, data=data)
    return e.msg_uid


def staged():
    return [e.to_msg() for e in StagedMessage.unsent()]
