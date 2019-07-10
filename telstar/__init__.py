"""
Telstar is a package to write producer and consumers groups against redis streams.
"""
__version__ = "0.1.4"

import logging

from .com import StagedMessage

log = logging.getLogger(__package__).addHandler(logging.NullHandler())


def stage(topic, data):
    e = StagedMessage.create(topic=topic, data=data)
    return e.msg_uid


def staged():
    return [e.to_msg() for e in StagedMessage.unsent()]
