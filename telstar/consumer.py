import json
import logging
import threading
import time
import uuid
from functools import partial

import redis

from .com import Message

# An important concept to understand here is the consumer group which give us the following consumer properties:
# msg   -> consumer
#
# msg:1 -> cg-userSignUp.1
# msg:2 -> cg-userSignUp.2
# msg:3 -> cg-userSignUp.3
# msg:4 -> cg-userSignUp.1
# msg:5 -> cg-userSignUp.2
# msg:6 -> cg-userSignUp.3
# msg:7 -> cg-userSignUp.1
#
# Which basically means that inside a group a single consumer will only get message the others have not yet seen.
# For a deep dive read to this -> https://redis.io/topics/streams-intro
# This allows us to create N consumers w/o needing to figure out which message already has been processed.

log = logging.getLogger(__name__)


class MultiConsumer(object):

    @staticmethod
    def increment(id):
        # IDs are of the form "1509473251518-0" and comprise a millisecond
        # timestamp plus a sequence number to differentiate within the timestamp.
        time, sequence = id.decode("ascii").split("-")
        if not sequence:
            raise Exception("Argument error, {id} has wrong format not #-#")
        next_sequence = int(sequence) + 1

        return bytes(f"{time}-{next_sequence}", "ascii")

    @staticmethod
    def decrement(id):
        time, sequence = id.decode("ascii").split("-")
        if not sequence:
            raise Exception("Argument error, {id} has wrong format not #-#")
        sequence = int(sequence)
        time = int(time)
        if sequence == 0:
            time = time - 1
        else:
            sequence = int(sequence) - 1

        return bytes(f"{time}-{sequence}", "ascii")

    def __init__(self, link: redis.Redis, group_name: str, consumer_name: str, config: dict, block=2000, claim_the_dead_after=20 * 1000):
        self.link = link
        self.block = block
        self.claim_the_dead_after = claim_the_dead_after
        self.consumer_name = consumer_name
        self.group_name = group_name

        self.processors = {f"telstar:stream:{stream_name}": fn
                           for stream_name, fn in config.items()}

        self.streams = self.processors.keys()
        for stream_name in self.streams:
            self.create_consumer_group(stream_name)

    def get_consumer_name(self, stream):
        return f"cg:{self.group_name}:{self.consumer_name}"

    def _seen_key(self, msg: Message):
        return f"telstar:seen:{msg.stream}:{self.group_name}:{msg.msg_uuid}"

    def _checkpoint_key(self, stream: str):
        return f"telstar:checkpoint:{stream}:{self.get_consumer_name(stream)}"

    # A new consumer group for the given stream, if the stream does not exist yet
    # create one (`mkstream`) - if it does we want all messages present `id=0`
    def create_consumer_group(self, stream_name: str):
        try:
            self.link.xgroup_create(stream_name, self.group_name, mkstream=True, id="0")
        except redis.exceptions.ResponseError:
            log.debug(f"Group: {self.group_name} for Stream: {stream_name} already exists")

    # In consumer groups, consumers can disappear, when they do they can leave non ack'ed message
    # which we want to claim and be delivered to a new consumer
    def claim_message_from_the_dead(self, stream_name: str):
        # Get information about all consumers in the group and how many messages are pending
        pending_info = self.link.xpending(stream_name, self.group_name)
        # {'pending': 10,
        #  'min': b'1560032216285-0',
        #  'max': b'1560032942270-0',
        #  'consumers': [{'name': b'cg-userSignUp.1', 'pending': 10}]}
        # Nothing to do
        if pending_info["pending"] == 0:
            log.debug(f"Stream: {stream_name} in Group: {self.group_name} has no pending messages")
            return
        # Get all messages ids within that range and select the ones we want to claim and claim them
        # But only if they are pending for more than 20secs.
        pending_messages = self.link.xpending_range(stream_name, self.group_name,
                                                    pending_info["min"], pending_info["max"], pending_info["pending"])
        # [
        #   {'message_id': b'1560194528886-0',
        #    'consumer': b'cg-userSignUp.1',
        #    'time_since_delivered': 22020,
        #    'times_delivered': 1}
        #  ...]
        messages_to_claim = [p["message_id"] for p in pending_messages]

        if not messages_to_claim:
            # The pending messages are all our own no need to claim anything
            # This can happen when we simply restart a consumer with the same name
            return

        # It might be cheaper to claim *and* receive the message so we can work on them directly
        # w/o catching up through the history with the potential of a lot of already seen keys.
        log.debug(f"Stream: {stream_name} in Group: {self.group_name} claiming: {len(messages_to_claim)} message(s)")
        claimed_messages = self.link.xclaim(stream_name, self.group_name, self.consumer_name, self.claim_the_dead_after, messages_to_claim, justid=True)
        log.debug(f"Stream: {stream_name} in Group: {self.group_name} claimed: {len(messages_to_claim)} message(s)")
        return claimed_messages

    # We claim the message from other dead/non-responsive consumers.
    # When new message have been claimed they are usually from the past
    # which means in order to process them we need to start processing our history.
    def transfer_and_process_stream_history(self, streams: list):
        last_seen = dict()
        for stream_name in streams:
            last_seen[stream_name] = self.get_last_seen_id(stream_name)
            stream_msg_ids = self.claim_message_from_the_dead(stream_name)
            if stream_msg_ids:
                # if there are message that we have claimed we need to determine where to start processing
                # because we can't just wait for new message to arrive.
                before_earliest = self.decrement(min(stream_msg_ids))
                next_after_seen = self.increment(last_seen[stream_name])
                last_seen[stream_name] = min([before_earliest, next_after_seen])
        # Read all message for the past up until now.
        log.info(f"Stream: {', '.join(last_seen)} in Group: {self.group_name} as Consumer: {self.consumer_name} reading past messages")
        self.catchup(last_seen)

    # This is the main loop where we start from the history
    # and claim message and reprocess our history.
    # We also loop the transfer_and_process_history as other consumers might have died while we waited
    def run(self):
        log.info(f"Starting consumer loop for Group {self.group_name}")
        while True:
            self._once()

    def _once(self):
        self.transfer_and_process_stream_history(self.streams)
        # With our history processes we can now start waiting for new message to arrive `>`
        config = {k: ">" for k in self.streams}
        log.info(f"Stream: {', '.join(self.streams)} in Group: {self.group_name} as Consumer: {self.consumer_name} reading pending message or waiting for new")
        self.read(config, block=self.block)

    def get_last_seen_id(self, stream_name: str):
        check_point_key = self._checkpoint_key(stream_name)
        return self.link.get(check_point_key) or b"0-0"

    # Multiple things are happening here.
    # 1. Save the stream_msg_id as checkpoint, which means
    #    that we know where to start should the consumer be restarted
    # 2. Each message has a UUID and in order to process each meassage only once we remember
    #    the UUID for 14 days
    # 3. Acknowledge the message to meaning that we have processed it
    def acknowledge(self, msg: Message, stream_msg_id):
        log.debug(f"Stream: telstar:stream:{msg.stream} in Group: {self.group_name} acknowledging Message: {msg.msg_uuid} - {stream_msg_id}")
        check_point_key = self._checkpoint_key(f"telstar:stream:{msg.stream}")
        seen_key = self._seen_key(msg)
        # Execute the following statments in a transaction e.g. redis speak `pipeline`
        pipe = self.link.pipeline()

        # If this key changes before we execute the pipeline than the ack fails and this the processor reverts all the work.
        # Which is exactly what we want in this case as the work has already been completed by another consumer.
        pipe.watch(seen_key)

        # Mark this as a seen key for 14 Days meaning if the message reappears after 14 days we reprocess it
        pipe.set(seen_key, 1, ex=14 * 24 * 60 * 60)  # 14 days

        # Set the checkpoint for this consumer so that it knows where to start agains once it restarts.
        pipe.set(check_point_key, stream_msg_id)

        # Acknowledge the actual message
        pipe.xack(f"telstar:stream:{msg.stream}", self.group_name, stream_msg_id)
        pipe.execute()

    def work(self, stream_name, stream_msg_id, record):
        msg = Message(stream_name,
                      uuid.UUID(record[Message.IDFieldName].decode("ascii")),
                      json.loads(record[Message.DataFieldName]))
        done = partial(self.acknowledge, msg, stream_msg_id)
        key = self._seen_key(msg)
        if self.link.get(key):
            # This is a double send
            log.debug(f"Stream: telstar:stream:{msg.stream} in Group: {self.group_name} skipping already processed Message: {msg.msg_uuid} - {stream_msg_id} ")
            return done()

        log.info(f"Stream: telstar:stream:{msg.stream} in Group: {self.group_name} processing Message: {msg.msg_uuid} - {stream_msg_id}")
        self.processors[stream_name.decode("ascii")](self, msg, done)

    # Process all message from `start`
    def catchup(self, streams):
        return self._xreadgroup(streams)

    # Process wait for new messages
    def read(self, streams, block):
        return self._xreadgroup(streams, block=block)

    def _xreadgroup(self, streams, block=0):
        processed = 0
        value = self.link.xreadgroup(self.group_name, self.consumer_name, streams, block=block)
        if not value:
            return 0
        for stream_name, records in value:
            for record in records:
                stream_msg_id, record = record
                self.work(stream_name, stream_msg_id, record)
                processed = processed + 1
        return processed


class Consumer(MultiConsumer):
    def __init__(self, link, group_name, consumer_name, stream_name, processor_fn):
        super().__init__(link, group_name, consumer_name, {stream_name: processor_fn})


class MultiConsumeOnce(MultiConsumer):
    # This lets you write code that is executed once against an entire stream
    # TODO: What would be really cool is to have this sort of like migrations
    #       where `telstar` itself has cli commands to add and run files containing
    #       `MultiConsumeOnce` code.
    def __init__(self, link: redis.Redis, group_name: str, config: dict):
        super().__init__(link, group_name, "once-consumer", config, 2000, 20000)

    def _applied_key(self):
        return f"telstar:once:{self.group_name}"

    def is_applied(self):
        key = self._applied_key()
        return bool(self.link.get(key))

    def mark_as_applied(self):
        key = self._applied_key()
        return self.link.set(key, int(time.time()))

    def has_pending_message(self):
        for stream_name in self.streams:
            if self.link.xpending(stream_name, self.group_name)["pending"] != 0:
                return True
        return False

    def run(self):
        num_processed = 0
        if self.is_applied():
            log.info(f"Group: {self.group_name} for Streams: {self.streams} will not run as it already ran")
            return num_processed

        # This is the first time we try to apply this.
        if not self.has_pending_message():
            # Reading a stream from ">" has a special meaning, it instructs redis to send all messages to the group
            # Which does two things first it puts them all into the pending list of that consumer inside the group
            # and also delivers them to the client.
            streams = {s: ">" for s in self.streams}
            num_processed = self.read(streams, 0)
        else:
            # Now the data as already been delivered to the group we can now start reading from the beginning
            streams = {s: "0" for s in self.streams}
            num_processed = self.read(streams, 0)

        # everything has been seen and processed where can mark this as applied
        if not self.has_pending_message():
            self.mark_as_applied()
        return num_processed


class ThreadedMultiConsumer:
    def __init__(self, link: redis.Redis, consumer_name: str, config: dict, **kw):
        self.consumers = list()
        for group_name, config in config.items():
            self.consumers.append(MultiConsumer(link, group_name, consumer_name, config, **kw))

    def run(self):
        threads = list()
        for c in self.consumers:
            t = threading.Thread(target=c.run, daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()