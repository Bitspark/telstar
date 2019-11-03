<p align="center">
  <a href="" rel="noopener">
 <img width=200px height=200px src="https://i.imgur.com/M2E5FvK.png" alt="Project logo"></a>
</p>

<h3 align="center">Telstar</h3>

<div align="center">

  [![Status](https://img.shields.io/badge/status-active-success.svg)]()
  [![CircleCI](https://circleci.com/gh/Bitspark/telstar.svg?style=svg)](https://circleci.com/gh/Bitspark/telstar)
  [![Pypi](https://img.shields.io/pypi/v/telstar.svg?style=svg)](https://pypi.org/project/telstar/)
  [![GitHub Issues](https://img.shields.io/github/issues/Bitspark/telstar.svg)](https://github.com/kylelobo/The-Documentation-Compendium/issues)
  [![GitHub Pull Requests](https://img.shields.io/github/issues-pr/Bitspark/telstar.svg)](https://github.com/kylelobo/The-Documentation-Compendium/pulls)
  [![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

<p align="center">
    This library is what came out of creating a distributed service architecture for one of our products.
    Telstar makes it easy to write consumer groups and producers against Redis streams.
    <br>
</p>

## 📝 Table of Contents

- [About](#about)
- [Getting Started](#getting_started)
- [Deployment](#deployment)
- [Usage](#usage)
- [Built Using](#built_using)
- [TODO](../TODO.md)
- [Contributing](../CONTRIBUTING.md)
- [Authors](#authors)
- [Acknowledgments](#acknowledgement)

## 🧐 About <a name = "about"></a>

To run our distributed architecture at [@bitspark](https://bitspark.de) we needed a way to produce and consume messages with an exactly-once delivery design.
We think that, by packing up our assumptions into a separate library, we can make it easy for other services to adhere to our initial design and/or comply with future changes.

Another aspect of open-sourcing this library is that we have not found a package that does what we needed. And as we use OSS in many critical parts of our infrastructure, we wanted to give back this project.

## 🏁 Getting Started <a name = "getting_started"></a>

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See [deployment](#deployment) for notes on how to deploy the project on a live system.

### Prerequisites

You will need `python >= 3.6` as we use type annotations and a running `Redis` server with at least version `>= 5.0`.

### Installing

A step by step series of examples that tell you how to get a development env running.
Install everything using `pip.`

```bash
pip install telstar
```

## 🔧 Running the tests <a name = "tests"></a>

This package comes with an end to end test, which simulates
a scenario with all sorts of failures that can occur during operation. Here is how to run the tests.

```bash
git clone git@github.com:Bitspark/telstar.git
pip install -r requirements.txt

./telstar/tests/test_telstar.sh
pytest --ignore=telstar/
```

## 🎈 Usage <a name="usage"></a>

This package uses consumer groups and Redis streams as a backend to deliver messages exactly once. To understand Redis streams and what the `Consumer` can do for you to read go and read [Redis Streams](https://redis.io/topics/)

### The Producer - how to get data into the system

Create a producer in our case this produces a different message with the same data every .5 seconds

```python
import Redis
import os
from uuid import uuid4
from time import sleep

from telstar.producer import Producer
from telstar.com import Message

link = redis.from_url("redis://")

def producer_fn():
    topic = "userSignedUp"
    uuid = uuid4() # Based on this value the system deduplicates messages
    data = dict(email="test1@example.com", field="value")
    msgs = [Message(topic, uuid, data)]

    def done():
        # Do something after all messages have been sent.
        sleep(.5)
    return msgs, done

Producer(link, producer_fn, context_callable=None).run()
```

Start the producer with the following command.

```bash
python producer.py
```

### The Consumer - how to get data out of the system

Now let's create consumer

```python
import Redis
import telstar

from marshmallow import EXCLUDE, Schema, fields

redis = redis.from_url("redis://")
consumer_name = "local.dev"

app = telstar.app(redis, consumer_name=consumer_name, block=20 * 1000)


class UserSchema(Schema):
    email = fields.Email(required=True)

    class Meta:
        unknown = EXCLUDE


@app.consumer("mygroup", ["userSignedUp"], schema=UserSchema, strict=True, acknowledge_invalid=False)
def consumer(record: dict):
    print(record)

if __name__ == "__main__":
    app.start()
```

Now start the consumer as follows:

```bash
python consumer.py
```

You should see output like the following.

```bash
{"email": "test1@example.com"}
{"email": "test1@example.com"}
{"email": "test1@example.com"}
```

Until the stream is exhausted.


## 🚀 Deployment <a name = "deployment"></a>

We currently use Kubernetes to deploy our producers and consumers as simple jobs, which, of course, is a bit suboptimal. It would be better to deploy them as a replica set.

## ⛏️ Built Using <a name = "built_using"></a>

- [Redis](https://redis.io/) - Swiss Army Knive
- [Peewee](http://docs.peewee-orm.com/en/latest/) - ORM for testing

## ✍️ Authors <a name = "authors"></a>

- [@kairichard](https://github.com/kairichard) - Idea & Initial work

See also the list of [contributors](https://github.com/Bitspark/telstar/contributors) who participated in this project.

## 🎉 Acknowledgements <a name = "acknowledgement"></a>

- Thanks for [@bitspark](https://github.com/Bitspark/) for allowing me to work on this.
- Inspiration/References and reading material
  - https://walrus.readthedocs.io/en/latest/streams.html
  - https://redis.io/topics/streams-intro
  - https://github.com/tirkarthi/python-redis-streams-playground/blob/master/consumer.py
  - https://github.com/brandur/rocket-rides-unified/blob/master/consumer.rb
  - https://brandur.org/redis-streams
  - https://github.com/coleifer/walrus
  - http://charlesleifer.com/blog/multi-process-task-queue-using-redis-streams/
  - http://charlesleifer.com/blog/redis-streams-with-python/
