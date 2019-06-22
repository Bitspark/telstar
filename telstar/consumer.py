# Strategies to build a consumer
# Requirements:
#   It must be able to deal with `at least once` delivery - any other guarantees are ridiculous
#   It must be able to start multiple instance of the consumer without reading messages twice off the stream
#
#   There should be no delays or intervalling in processing the incoming data.
#   Processing the data should be transactional meaning that if we fail to proccess the message it should not be marked as read.
#   It must have strong error reporting as we want to be immdiatly informed if something goes haywire.
#
# Verification:
#   * see tests/test.sh
#
# Read:
#   https://walrus.readthedocs.io/en/latest/streams.html
#   https://redis.io/topics/streams-intro
#   https://github.com/tirkarthi/python-redis-streams-playground/blob/master/consumer.py
#
#   https://github.com/brandur/rocket-rides-unified/blob/master/consumer.rb
#   https://brandur.org/redis-streams
#
#   https://github.com/coleifer/walrus
#   http://charlesleifer.com/blog/multi-process-task-queue-using-redis-streams/
#   http://charlesleifer.com/blog/redis-streams-with-python/

import json
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


class Consumer(object):

    @staticmethod
    def increment(id):
        # IDs are of the form "1509473251518-0" and comprise a millisecond
        # timestamp plus a sequence number to differentiate within the timestamp.
        time, sequence = id.decode("ascii").split("-")
        if not sequence:
            raise Exception("Argument error, {id} has wrong format not #-#")
        next_sequence = int(sequence) + 1

        return bytes(f"{time}-{next_sequence}", "ascii")

    def __init__(self, link, group_name, consumer_name, stream_name, processor_fn):
        self.link = link
        self.stream_name = f"telstar:stream:{stream_name}"
        self.consumer_name = consumer_name
        self.group_name = group_name

        self.processor_fn = processor_fn
        self.consumer_group = f"{self.stream_name}:{self.group_name}"
        self.consumer_name = f"cg:{self.consumer_group}:{self.consumer_name}"
        self.create_consumer()

    def _seen_key(self, msg: Message):
        return f"telstar:seen:{self.consumer_group}:{msg.msg_uuid}"

    def _checkpoint_key(self):
        return f"telstar:checkpoint:{self.consumer_name}"

    # A new consumer group for the given stream, if the stream does not exist yet
    # create one (`mkstream`) - if it does we want all messages present `id=0`
    def create_consumer(self):
        try:
            self.link.xgroup_create(self.stream_name, self.group_name, mkstream=True, id="0")
        except redis.exceptions.ResponseError:
            pass  # The group already exists

    # In consumer groups, consumers can disappear, when they do they can leave non ack'ed message
    # which we want to claim and be delivered to us.
    def claim_message_from_the_dead(self):
        # Get information about all consumers in the group and how many messages are pending
        pending_info = self.link.xpending(self.stream_name, self.group_name)
        # {'pending': 10,
        #  'min': b'1560032216285-0',
        #  'max': b'1560032942270-0',
        #  'consumers': [{'name': b'cg-userSignUp.1', 'pending': 10}]}

        # Nothing to see here
        if pending_info["pending"] == 0:
            return

        # Now get all messages ids within that range and select the ones we want to claim and claim them
        # But only if they are pending for more than 20secs.
        pending_messages = self.link.xpending_range(self.stream_name, self.group_name,
                                                    pending_info["min"], pending_info["max"], pending_info["pending"])
        # [
        #   {'message_id': b'1560194528886-0',
        #    'consumer': b'cg-userSignUp.1',
        #    'time_since_delivered': 22020,
        #    'times_delivered': 1}
        #  ...]
        messages_to_claim = [p["message_id"] for p in pending_messages if not p["consumer"].decode("ascii") == self.consumer_name]
        if not messages_to_claim:
            return  # The pending messages are all our own no need to claim anything
        return self.link.xclaim(self.stream_name, self.group_name, self.consumer_name, 20 * 1000, messages_to_claim, justid=True)

    # We claim the message from other dead/non-responsive consumers.
    # When new message have been claimed they are usually from the past which
    # which means in order to process them we need to start processing our history.
    def transfer_and_process_history(self):
        start = self.get_last_seen_id()
        stream_msg_ids = self.claim_message_from_the_dead()
        if stream_msg_ids:
            start = min([min(stream_msg_ids), self.increment(start)])
            print(start)
            # start = b"0-0"  # This is strange is it means we want to reprocess everything - including what we have seen sofar
        self.catchup(start=start)

    # This is the main loop where we start from the history
    # and claim message and reprocess our history.
    def run(self):
        self.transfer_and_process_history()
        while True:
            self.subscribe()  # block for 2 secs.
            self.transfer_and_process_history()

    def get_last_seen_id(self):
        check_point_key = self._checkpoint_key()
        return self.link.get(check_point_key) or b"0-0"

    # Multiple things are happening here.
    # 1. Save the stream_msg_id as checkpoint, which means
    #    that we know where to start should the consumer be restarted
    # 2. Each message has a UUID and in order to process each meassage only once we remember
    #    the UUID for 14 days
    # 3. Acknowledge the message to meaning that we have processed it
    def acknowledge(self, msg: Message, stream_msg_id):
        check_point_key = self._checkpoint_key()
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
        pipe.xack(self.stream_name, self.group_name, stream_msg_id)
        pipe.execute()

    def work(self, stream_msg_id, record):
        msg = Message(self.stream_name, uuid.UUID(record[Message.IDFieldName].decode("ascii")),
                      json.loads(record[Message.DataFieldName]))
        key = f"telstar:seen:{self.consumer_group}:{msg.msg_uuid}"
        if self.link.get(key):
            # This is a double send
            self.acknowledge(msg, stream_msg_id)
            return

        self.processor_fn(self, msg, done=partial(self.acknowledge, msg, stream_msg_id))

    # Process all message from `start`
    def catchup(self, start):
        return self._xread(start)

    # Process wait for new messages
    def subscribe(self):
        return self._xread(">", block=2000)

    def _xread(self, start, block=0):
        value = self.link.xreadgroup(self.group_name, self.consumer_name, {self.stream_name: start}, block=block)
        if not value:
            return 0
        [[_, records]] = value
        for record in records:
            stream_msg_id, record = record
            self.work(stream_msg_id, record)
        return len(records)
