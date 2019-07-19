import redis
from functools import reduce


class AdminMessage:
    def __init__(self, message_id: bytes, consumer: str, time_since_delivered: int, times_delivered: int):
        self.message_id = message_id
        self.consumer = consumer
        self.time_since_delivered = time_since_delivered
        self.times_delivered = times_delivered


class Consumer:
    def __init__(self, name: bytes, pending: int, idle: int):
        self.name = name
        self.pending_messages = pending
        self.idle_time = idle


class Stream:
    def __init__(self, stream_name, link: redis.Redis):
        self.stream_name = stream_name
        self.link = link

    def get_groups(self):
        return [Group(self.link, name=info["name"], stream_name=self.stream_name, **self.link.xpending(self.stream_name, info["name"]))
                for info in self.link.xinfo_groups(self.stream_name)]

    def get_pending_messages(self):
        return sum([g.get_pending_messages() for g in self.get_groups()], [])


class Group:
    def __init__(self, link: redis.Redis, name: str, stream_name: str, pending: int, min, max, consumers):
        self.link = link
        self.name = name
        self.stream_name = stream_name
        self.pending, self.min, self.max, self.consumers = pending, min, max, consumers

    def get_pending_messages(self):
        if self.pending == 0:
            return []
        return [AdminMessage(**info)
                for info in self.link.xpending_range(self.stream_name, self.name, self.min, self.max, self.pending)]

    def get_consumers(self):
        return [Consumer(**info) for info in self.link.xinfo_consumers(self.stream_name, self.name)]


class admin:
    def __init__(self, link: redis.Redis):
        self.link: redis.Redis = link

    def get_streams(self, match=None):
        match = match or ""
        streams = self.link.scan_iter(match=f"telstar:stream:{match}*")
        return [Stream(s, self.link) for s in streams]

    def get_consumers(self):
        return sum([g.get_consumers() for s in self.get_streams() for g in s.get_groups()], [])
