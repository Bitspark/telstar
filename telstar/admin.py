import json
import uuid
from typing import Dict, List, Optional, Tuple, Union

import redis

from .com import Message, decrement_msg_id


class admin:
    def __init__(self, link: redis.Redis) -> None:
        self.link: redis.Redis = link

    def get_streams(self, match: None = None) -> List["Stream"]:
        match = match or ""
        # We wanted to use `scan_iter` but for an yet unknown reason
        # `SCAN` take hundreds of iterations to find all streams.
        streams = self.link.keys(f"telstar:stream:{match}*")
        return [Stream(self, s) for s in streams]

    def get_consumers(self) -> List["Consumer"]:
        return sum([g.get_consumers() for s in self.get_streams() for g in s.get_groups()], [])


class Stream:
    def __init__(self, admin: admin, stream_name: str) -> None:
        self.name = stream_name
        self.admin = admin
        self.link = admin.link

    @property
    def display_name(self) -> bytes:
        return self.name.replace(b"telstar:stream:", b"")

    def get_groups(self) -> List["Group"]:
        return [Group(self, name=info["name"], **self.link.xpending(self.name, info["name"]))
                for info in self.link.xinfo_groups(self.name)]

    def get_pending_messages(self) -> List["AdminMessage"]:
        return sum([g.get_pending_messages() for g in self.get_groups()], [])

    def get_length(self) -> int:
        return self.link.xlen(self.name)


class Group:
    def __init__(self, stream: Stream, name: str, pending: int, min: Optional[bytes], max: Optional[bytes], consumers: List[Dict[str, Union[bytes, int]]]) -> None:
        self.stream = stream
        self.link = stream.link
        self.name = name
        self.pending, self.min, self.max, self.consumers = pending, min, max, consumers

    def get_pending_messages(self) -> List["AdminMessage"]:
        if self.pending == 0:
            return []
        return [AdminMessage(self, **info)
                for info in self.link.xpending_range(self.stream.name, self.name, self.min, self.max, self.pending)]

    def get_consumers(self) -> List["Consumer"]:
        return [Consumer(self, **info) for info in self.link.xinfo_consumers(self.stream.name, self.name)]

    def get_seen_messages(self) -> int:
        stream_name = self.stream.name.replace(b"telstar:stream:", b"").decode("ascii")
        name = self.name.decode("ascii")
        return len(self.link.keys(f"telstar:seen:{stream_name}:{name}*"))

    def delete(self) -> bool:
        return self.link.xgroup_destroy(self.stream.name, self.name)


class Consumer:
    def __init__(self, group: Group, name: bytes, pending: int, idle: int) -> None:
        self.group = group
        self.name = name
        self.pending_messages = pending
        self.idle_time = idle

    def delete(self) -> int:
        return self.group.stream.admin.link.xgroup_delconsumer(self.group.stream.name, self.group.name, self.name)


class AdminMessage:
    def __init__(self, group: Group, message_id: bytes, consumer: str, time_since_delivered: int, times_delivered: int) -> None:
        self.group = group
        self.message_id = message_id
        self.consumer = consumer
        self.time_since_delivered = time_since_delivered
        self.times_delivered = times_delivered

    def remove(self):
        pipe = self.group.stream.admin.link.pipeline()
        pipe.xack(self.group.stream.name, self.group.name, self.message_id)
        pipe.xdel(self.group.stream.name, self.message_id)
        pipe.execute()

    def read_raw(self) -> List[List[Union[bytes, List[Tuple[bytes, Dict[bytes, bytes]]]]]]:
        return self.group.stream.admin.link.xread({
            self.group.stream.name: decrement_msg_id(self.message_id)
        }, count=1)

    def read(self) -> Message:
        for stream_name, records in self.read_raw():
            for record in records:
                stream_msg_id, record = record
                return Message(stream_name,
                               uuid.UUID(record[Message.IDFieldName].decode("ascii")),
                               json.loads(record[Message.DataFieldName]))
