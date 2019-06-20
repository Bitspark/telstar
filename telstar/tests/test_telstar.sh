#!/bin/bash

# This script is used to verify/test a assumpations and guaranteesabout the system. 
# 
# Todos: 
#   * kill redis in the process
#   * kill mysql in the process
#   * use more fuzzing - meaning kill and start processes more randomly to simulate failure
#   * use this inside a CI
#   * improve the sleeping, maybe look at how we can wait until all pending messages are done and then make final diff.


readonly SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

function finish() {
    jobs -p | xargs kill
}

# Upon exit kill all child procceses
trap finish SIGINT
trap finish EXIT

# Configuration
export STREAM_NAME="${STREAM_NAME:-test}"
export GROUP_NAME="${GROUP_NAME:-validation}"

export SLEEPINESS="${SLEEPINESS:-10}"

export REDIS_PASSWORD="${REDIS_PASSWORD:-}"
export REDIS_PORT="${REDIS_PORT:-6379}"
export REDIS_HOST="${REDIS_HOST:-localhost}"
export REDIS_DB="${REDIS_DB:-10}"

export DATABASE="${DATABASE:-mysql://root:root@127.0.0.1:3306/test}"

export PYTHONPATH="${PYTHONPATH}:${SCRIPTPATH}../}"

if [ -x "$(command -v redis-cli)" ]; then
    # Clear redis only if available
    echo "flushing"
    redis-cli -n $REDIS_DB FLUSHDB 
fi


# Start the first customer with `create` which drops and creates the needed tables in mysql
CONSUMER_NAME=1 python $SCRIPTPATH/test_consumer.py setup &
CONSUMER_1=$! # This saves the PID of the last command - which we can use to `kill` the process later

# What for the creation to be done
sleep 2

# Start more consumers
CONSUMER_NAME=2 python $SCRIPTPATH/test_consumer.py &
CONSUMER_2=$!

CONSUMER_NAME=3 python $SCRIPTPATH/test_consumer.py &
CONSUMER_3=$!

# Create a producer that double sends messages because it does not delete them after sending. 
KEEPEVENTS=1 RANGE_FROM=1 RANGE_TO=101 python $SCRIPTPATH/test_producer.py setup create &
PRODUCER_1=$!

sleep 2

# Create another producer that keeps double sending
KEEPEVENTS=1 python $SCRIPTPATH/test_producer.py &
PRODUCER_2=$!

# Since CONSUMER_1 is in the background in already has consumed some messages from the stream
kill $CONSUMER_1 

# Wait some more for  CONSUMER_2 and CONSUMER_3 to process more data.
sleep 2

# Let's kill the producer while it is sending
kill $PRODUCER_1
kill $PRODUCER_2

# Now we restart CONSUMER_1
CONSUMER_NAME=1 python $SCRIPTPATH/test_consumer.py &
CONSUMER_1=$!

# Let all consumers process a bit more data
sleep 5


# Kill more consumers
kill $CONSUMER_1
kill $CONSUMER_3

# Oops all consumers are dead now
kill $CONSUMER_2

# Create another producer that generates the rest of the data but now also delete what was send already
RANGE_FROM=101 RANGE_TO=201 python $SCRIPTPATH/test_producer.py create &
PRODUCER_3=$!


# Start a new one
CONSUMER_NAME=4 python $SCRIPTPATH/test_consumer.py &
CONSUMER_4=$!

# Wait for `CONSUMER_4` to process all data
sleep 30
kill $CONSUMER_4
kill $PRODUCER_3

# Restart `CONSUMER_4` and kill it later
CONSUMER_NAME=4 python $SCRIPTPATH/test_consumer.py &
CONSUMER_4=$!

sleep 30
kill $CONSUMER_4

# Create a new consumer group that reads everything from the beginning of time.
SLEEPINESS=1 CONSUMER_NAME=4 GROUP_NAME=validation2 python $SCRIPTPATH/test_consumer.py &
NEW_CONSUMER=$!

sleep 10 
kill $NEW_CONSUMER

# Verify that the database hold the expected records
python  $SCRIPTPATH/_dbcontent.py | diff $SCRIPTPATH/expected.txt - || exit 1