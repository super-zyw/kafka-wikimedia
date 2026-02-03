from datetime import datetime, timedelta
from time import time
import pandas as pd
from quixstreams import Application
import configparser
import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from collections import deque
from threading import Thread



# read config from the config.ini file
config_obj = configparser.ConfigParser()
config = config_obj.read('config.ini')


# Define an application that will connect to Kafka
app = Application(
    broker_address=config_obj['topic']['bootstrap.servers'],  # Kafka broker address
)


with open(config_obj['schema_registry']['schema_path'], 'r') as file:
    schema_str = json.loads(file.read())
print(schema_str)

import getopt
import json
import logging
from confluent_kafka import Consumer, KafkaException
import configparser
from helper import *


# read config from the config.ini file
config_obj = configparser.ConfigParser()
config = config_obj.read('config.ini')


topic = config_obj['monitor']['topic']
# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {
    'bootstrap.servers'         : config_obj['monitor']['bootstrap.servers'],
    'group.id'                  : config_obj['monitor']['group.id'],
    'session.timeout.ms'        : config_obj['monitor']['session.timeout.ms'],
    'auto.offset.reset'         : config_obj['monitor']['auto.offset.reset'],
    'auto.commit.interval.ms'   : config_obj['monitor']['auto.commit.interval.ms'],
    'enable.auto.offset.store'  : True if config_obj['monitor']['enable.auto.offset.store'] == 'True' else False,
    'enable.auto.commit'        : True if config_obj['monitor']['enable.auto.commit'] == 'True' else False
    #'group.remote.assignor'     : config_obj['consumer']['group.remote.assignor']
}


# Create logger for consumer (logs will be emitted when poll() is called)
logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

# Create Consumer instance
consumer = Consumer(conf, logger=logger)
consumer.subscribe([topic], on_assign=print_assignment)

buffer = deque(maxlen=10)

def collect(key, data):
    buffer.append({
        "ts": datetime.now(),
        "key" : key,
        "count": data["value"]
    })

fig, ax = plt.subplots()

def update(frame):
    if not buffer:
        return

    df = pd.DataFrame(buffer)

    ax.clear()

    for key, g in df.groupby("key"):
        ax.plot(g["ts"], g["count"], label=key)

    ax.set_title("Event count per key (10s tumbling window)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Count")
    ax.legend(loc="upper left")


def consume():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None or msg.value() is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            data = json.loads(msg.value())
            collect(str(msg.key()), data)
            consumer.commit(msg)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


Thread(target=consume, daemon=True).start()
ani = FuncAnimation(fig, update, interval=100)
plt.show()






