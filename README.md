<p align="center">
  <a href="" rel="noopener">
 <img width=200px height=200px src="https://i.imgur.com/6wj0hh6.jpg" alt="Project logo"></a>
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
    Telstar makes it easy to write consumer groups and producers against redis streams.
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
In order to run our distributed architecture we needed a way to receive and produces messages with a an almost exactly once delivery design.
We think that by packing up our assumptions into a seperated lib we make easy for other service to adhere to our intial design and/or comply to future changes.

Another aspect why we put this is out is, that we have not found a package that does what we needed. And as we ourselves use quite a lot OSS we wanted to give back this project.

## 🏁 Getting Started <a name = "getting_started"></a>
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See [deployment](#deployment) for notes on how to deploy the project on a live system.

### Prerequisites
You will need `python >= 3.6` as we use type annotations and a running `redis` server with at least version `>= 5.0`.

### Installing
A step by step series of examples that tell you how to get a development env running.
Install everything using `pip`

```
pip install telstar
```

#### The Producer - how to get data into the system

Create a producer in our case this produces different message with the same data every .5 seconds
```python
import redis
import os
from uuid import uuid4
from time import sleep

from telstar.producer import Producer
from telstar.com import Message

link = redis.from_url(os.environ["REDIS"])

def producer_fn():
    topic = "mytopic"
    uuid = uuid4() # Based on this value the system deduplicates messages
    data = dict(key="value")
    msgs = [Message(topic, uuid, data)]

    def done():
        # Do something after all messages have been sent.
        sleep(.5)
    return msgs, done

Producer(link, producer_fn, context_callable=None).run()
```
Start the producer with the following command

```bash
REDIS=redis:// python producer.py
```

#### The Consumer - how to get data out of the system
Now lets creates consumer

```python
import redis
from telstar.consumer import Consumer



link = redis.from_url(os.environ["REDIS"])

def consumer(consumer, record, done):
    print(record.__dict__)
    done() # You need to call done otherwise we will get the same message over and over again

Consumer(link=link, group_name="mygroup", consumer_name="consumer-1",
         config={"mystream": consumer}).run()

```

Now start the consumer as follows:
```bash
REDIS=redis:// python consumer.py
```
You should see output like the following
```bash
{'stream': 'telstar:stream:mytopic', 'msg_uuid': b'64357230-8ff3-4eb9-8c06-757a42e961e2', 'data': {'key': 'value'}}
{'stream': 'telstar:stream:mytopic', 'msg_uuid': b'a2443ae2-549e-49ab-9256-3f13575ba6ae', 'data': {'key': 'value'}}
{'stream': 'telstar:stream:mytopic', 'msg_uuid': b'1c1d9f8c-e37b-43be-bab2-45caa10f233d', 'data': {'key': 'value'}}
{'stream': 'telstar:stream:mytopic', 'msg_uuid': b'5da942ab-4aec-4a7f-bd93-a97168a8d3ad', 'data': {'key': 'value'}}
```
Until the stream is exhausted.

## 🔧 Running the tests <a name = "tests"></a>
This package comes with an end to end test which simulates
a scenario with all sorts of failures that can occur during operation. Here is how to run the tests.

```
git clone git@github.com:Bitspark/telstar.git
pip install -r requirements.txt
./telstar/tests/test_telstar.sh
```

## 🎈 Usage <a name="usage"></a>
This package uses consumer groups and redis streams as a backend to deliver messages exactly once. In order to understand redis streams and what the `Consumer` can do for you to read -> https://redis.io/topics/streams-intro

## 🚀 Deployment <a name = "deployment"></a>
We currently use Kubernetes to deploy our produces and consumers as simple jobs, which of course is a bit suboptimal, it would be better to deploy them as replica set.

## ⛏️ Built Using <a name = "built_using"></a>
- [Redis](https://redis.io/) - Swiss Army Knive
- [Peewee](http://docs.peewee-orm.com/en/latest/) - ORM for testing

## ✍️ Authors <a name = "authors"></a>
- [@kairichard](https://github.com/kairichard) - Idea & Initial work

See also the list of [contributors](https://github.com/Bitspark/telstar/contributors) who participated in this project.

## 🎉 Acknowledgements <a name = "acknowledgement"></a>
- Thanks for [@bitspark](https://github.com/Bitspark/) for giving me the oppertunity to work on this.
- Inspiration/References and reading material
  - https://walrus.readthedocs.io/en/latest/streams.html
  - https://redis.io/topics/streams-intro
  - https://github.com/tirkarthi/python-redis-streams-playground/blob/master/consumer.py
  - https://github.com/brandur/rocket-rides-unified/blob/master/consumer.rb
  - https://brandur.org/redis-streams
  - https://github.com/coleifer/walrus
  - http://charlesleifer.com/blog/multi-process-task-queue-using-redis-streams/
  - http://charlesleifer.com/blog/redis-streams-with-python/
