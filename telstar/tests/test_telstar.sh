#!/bin/bash
# This script is used to verify/test a assumpations and guaranteesabout the system.
#
# Todos:
#   * kill redis in the process
#   * kill mysql in the process
#   * use more fuzzing - meaning kill and start processes more randomly to simulate failure
#   * improve the sleeping, maybe look at how we can wait until all pending messages are done and then make final diff.

# Configuration
export STREAM_NAME="${STREAM_NAME:-mystream}"
export STREAM_NAME_TWO="${STREAM_NAME_TWO:-mystream2}"

export GROUP_NAME="${GROUP_NAME:-validation}"

export SLEEPINESS="${SLEEPINESS:-10}"

export REDIS="${REDIS:-redis://localhost:6379/10}"
export DATABASE="${DATABASE:-mysql://root:root@127.0.0.1:3306/test}"

export PYTHONPATH="${PYTHONPATH}:${SCRIPTPATH}../}"
export PYTHONUNBUFFERED=True

export ORM="${ORM:-peewee}"

readonly SCRIPTPATH="$(
    cd "$(dirname "$0")"
    pwd -P
)"

export CONSUMER_SCRIPT="${SCRIPTPATH}/${ORM}_test_consumer.py"
export PRODUCER_SCRIPT="${SCRIPTPATH}/${ORM}_test_producer.py"

function kill_childs_and_exit() {
    echo "Attempting to kill all childs"
    echo "..."
    pkill -P $$
    echo "ok, bye"
    exit 1
}

# TRAP CTRL-C and kill all childs
trap kill_childs_and_exit INT

if [ -x "$(command -v redis-cli)" ]; then
    # Clear redis only if available
    echo "flushing"
    redis-cli -u $REDIS FLUSHDB
fi

main() {

    # Start the first customer with `setup` which drops and creates the needed tables in mysql
    CONSUMER_NAME=1 python $CONSUMER_SCRIPT setup &
    CONSUMER_1=$! # This saves the PID of the last command - which we can use to `kill` the process later

    # What for the creation to be done
    sleep 2

    # Start more consumers
    # The `SLEEPINESS` will result in race conditions in the database, which is what we want for testing
    SLEEPINESS=20 CONSUMER_NAME=2 python $CONSUMER_SCRIPT &
    CONSUMER_2=$!

    CONSUMER_NAME=3 python $CONSUMER_SCRIPT &
    CONSUMER_3=$!

    # Create a producer that double sends messages because it does not delete them after sending.
    KEEPEVENTS=1 RANGE_FROM=1 RANGE_TO=101 python $PRODUCER_SCRIPT setup create &
    PRODUCER_1=$!

    sleep 2

    # Create another producer that keeps double sending
    KEEPEVENTS=1 python $PRODUCER_SCRIPT &
    PRODUCER_2=$!

    # Create a producer that emits messages onto the second stream
    RANGE_FROM=1 RANGE_TO=101 STREAM_NAME=$STREAM_NAME_TWO python $PRODUCER_SCRIPT create &
    PRODUCER_TWO=$!

    # Since CONSUMER_1 is in the background in already has consumed some messages from the stream
    kill -0 $CONSUMER_1 && kill -9 $CONSUMER_1

    # Wait some more for  CONSUMER_2 and CONSUMER_3 to process more data.
    sleep 2

    # Let's kill the producer while it is sending
    kill -0 $PRODUCER_1 && kill -9 $PRODUCER_1
    kill -0 $PRODUCER_2 && kill -9 $PRODUCER_2

    # Now we restart CONSUMER_1
    CONSUMER_NAME=1 python $CONSUMER_SCRIPT &
    CONSUMER_1=$!

    # Let all consumers process a bit more data
    sleep 5

    # Kill more consumers
    kill -0 $CONSUMER_1 && kill -9 $CONSUMER_1
    kill -0 $CONSUMER_3 && kill -9 $CONSUMER_3

    # Oops all consumers are dead now
    kill $CONSUMER_2

    # Create another producer that generates the rest of the data but now also delete what was send already
    RANGE_FROM=101 RANGE_TO=201 python $PRODUCER_SCRIPT create &
    PRODUCER_3=$!

    # Start a new one
    CONSUMER_NAME=4 python $CONSUMER_SCRIPT &
    CONSUMER_4=$!

    # Wait for `CONSUMER_4` to process all data
    sleep 30
    kill -0 $CONSUMER_4 && kill -9 $CONSUMER_4
    kill -0 $PRODUCER_3 && kill -9 $PRODUCER_3

    kill -0 $PRODUCER_TWO && kill -9 $PRODUCER_TWO

    # Restart `CONSUMER_4` and kill it later
    CONSUMER_NAME=4 python $CONSUMER_SCRIPT &
    CONSUMER_4=$!

    sleep 30
    kill -0 $CONSUMER_4 && kill -9 $CONSUMER_4

    # Create a new consumer group that reads everything from the beginning of time.
    SLEEPINESS=1 CONSUMER_NAME=4 GROUP_NAME=validation2 python $CONSUMER_SCRIPT &
    NEW_CONSUMER=$!

    sleep 10
    kill -0 $NEW_CONSUMER && kill -9 $NEW_CONSUMER

    # Verify that the database hold the expected records
    python $SCRIPTPATH/_dbcontent.py | diff $SCRIPTPATH/expected.txt - || exit 1

    # Makre sure we do not have pending message left
    PENDING_STREAM_1=$(redis-cli -u $REDIS --csv XPENDING telstar:stream:$STREAM_NAME validation)
    PENDING_STREAM_2=$(redis-cli -u $REDIS --csv XPENDING telstar:stream:$STREAM_NAME_TWO validation)
    PENDING_STREAM_3=$(redis-cli -u $REDIS --csv XPENDING telstar:stream:$STREAM_NAME_TWO validation2)

    test "$PENDING_STREAM_1" == "0,NULL,NULL,NULL" || { echo "$STREAM_NAME validation has pending records" && exit 1; }
    test "$PENDING_STREAM_2" == "0,NULL,NULL,NULL" || { echo "$STREAM_NAME validation2 has pending records" && exit 1; }
    test "$PENDING_STREAM_3" == "0,NULL,NULL,NULL" || { echo "$STREAM_NAME_TWO validation2 has pending records" && exit 1; }
}
main
