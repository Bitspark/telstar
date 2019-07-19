import redis
from .com import Message


class AdminMessage:
    def __init__(self, message: Message):
        self.message: Message = message

    def mark_as_seen(self):
        pass

    def ack(self):
        pass

    def remove(self):
        pass


class Consumer:
    def remove(self):
        pass


class Stream:
    def trim(self, size):
        pass

    def get_pending_message(self):
        pass


class Group:
    def remove(self):
        pass


class admin:
    def __init__(self, link: redis.Redis):
        self.link: redis.Redis = link

    def get_streams(self):
        pass

    def get_consumers(self):
        pass

    def get_groups(self):
        pass

    def get_dead_groups(self):
        pass

    def get_dead_consumers(self):
        pass

    def get_broken_messages(self):
        pass
