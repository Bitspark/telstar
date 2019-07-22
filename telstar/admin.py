from .com import decrement_msg_id, Message
import redis
import uuid
import json


class admin:
    def __init__(self, link: redis.Redis):
        self.link: redis.Redis = link

    def get_streams(self, match=None):
        match = match or ""
        streams = self.link.scan_iter(match=f"telstar:stream:{match}*")
        return [Stream(self, s) for s in streams]

    def get_consumers(self):
        return sum([g.get_consumers() for s in self.get_streams() for g in s.get_groups()], [])


class Stream:
    def __init__(self, admin: admin, stream_name: str):
        self.name = stream_name
        self.admin = admin
        self.link = admin.link

    @property
    def display_name(self):
        return self.name.replace(b"telstar:stream:", b"")

    def get_groups(self):
        return [Group(self, name=info["name"], **self.link.xpending(self.name, info["name"]))
                for info in self.link.xinfo_groups(self.name)]

    def get_pending_messages(self):
        return sum([g.get_pending_messages() for g in self.get_groups()], [])

    def get_length(self):
        return self.link.xlen(self.name)


class Group:
    def __init__(self, stream: Stream, name: str, pending: int, min, max, consumers):
        self.stream = stream
        self.link = stream.link
        self.name = name
        self.pending, self.min, self.max, self.consumers = pending, min, max, consumers

    def get_pending_messages(self):
        if self.pending == 0:
            return []
        return [AdminMessage(self, **info)
                for info in self.link.xpending_range(self.stream.name, self.name, self.min, self.max, self.pending)]

    def get_consumers(self):
        return [Consumer(self, **info) for info in self.link.xinfo_consumers(self.stream.name, self.name)]

    def get_seen_messages(self):
        stream_name = self.stream.name.replace(b"telstar:stream:", b"").decode("ascii")
        name = self.name.decode("ascii")
        return len(self.link.keys(f"telstar:seen:{stream_name}:{name}*"))

    def delete(self):
        return self.link.xgroup_destroy(self.stream.name, self.name)


class Consumer:
    def __init__(self, group: Group, name: bytes, pending: int, idle: int):
        self.group = group
        self.name = name
        self.pending_messages = pending
        self.idle_time = idle

    def delete(self):
        return self.group.stream.admin.link.xgroup_delconsumer(self.group.stream.name, self.group.name, self.name)


class AdminMessage:
    def __init__(self, group: Group, message_id: bytes, consumer: str, time_since_delivered: int, times_delivered: int):
        self.group = group
        self.message_id = message_id
        self.consumer = consumer
        self.time_since_delivered = time_since_delivered
        self.times_delivered = times_delivered

    def read_raw(self):
        return self.group.stream.admin.link.xread({
            self.group.stream.name: decrement_msg_id(self.message_id)
        }, count=1)

    def read(self):
        for stream_name, records in self.read_raw():
            for record in records:
                stream_msg_id, record = record
                return Message(stream_name,
                               uuid.UUID(record[Message.IDFieldName].decode("ascii")),
                               json.loads(record[Message.DataFieldName]))
