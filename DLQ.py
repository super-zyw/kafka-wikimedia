import json
import sys
import time
from confluent_kafka import Producer
import json
import logging
import sys
import configparser


config_obj = configparser.ConfigParser()
config = config_obj.read('config/config.ini')

DLQ_TOPIC = "wikimedia.DLQ"

# ------------------------------------------------------------------
# DLQ Producer
# ------------------------------------------------------------------
dlq_producer = Producer({
    'bootstrap.servers'                     : config_obj['topic']['bootstrap.servers'], # local host
    'enable.idempotence'                    : True if config_obj['dlq']['enable.idempotence'] == '1' else False,
    'max.in.flight.requests.per.connection' : config_obj['dlq']['max.in.flight.requests.per.connection'],
    'acks'                                  : config_obj['dlq']['acks'],
    'delivery.timeout.ms'                   : int(config_obj['dlq']['delivery.timeout.ms']),
    'linger.ms'                             : int(config_obj['dlq']['linger.ms']),
    'batch.size'                            : int(config_obj['dlq']['batch.size']),
    'compression.type'                      : config_obj['dlq']['compression.type']
})

def send_to_dlq(msg, error):
    dlq_payload = {
        "original_topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": msg.key().decode("utf-8") if msg.key() else None,
        "error": str(error),
        "value": msg.value().decode("utf-8"),
        "timestamp": int(time.time() * 1000),
    }

    dlq_producer.produce(
        topic=DLQ_TOPIC,
        value=json.dumps(dlq_payload).encode("utf-8"),
    )
    dlq_producer.flush()
