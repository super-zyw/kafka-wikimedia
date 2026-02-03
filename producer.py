from confluent_kafka import Producer
import json
import logging
import sys
import time
from evenetHandler import EventHandler
from fake_useragent import UserAgent
from sseclient import SSEClient as EventSource
import configparser
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

# read config from the config.ini file
config_obj = configparser.ConfigParser()
config = config_obj.read('config.ini')

topic = config_obj['topic']['name']
url_string = str(config_obj['producer']['url'])

schema_registry_conf = {"url": config_obj["schema_registry"]["host"]}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

with open(config_obj['schema_registry']['schema_path'], 'r') as file:
    schema_str = file.read()
print(f"schema: {schema_str}")

string_serializer = StringSerializer('utf_8')
#json_serializer = JSONSerializer(schema_str, schema_registry_client, to_dict=lambda obj, ctx: obj)
#avro_serializer = AvroSerializer(schema_registry_client, schema_str, to_dict=lambda obj, ctx: obj)

# Producer setup
producer_config = {
    'bootstrap.servers'                     : config_obj['topic']['bootstrap.servers'], # local host
    'enable.idempotence'                    : True if config_obj['producer']['enable.idempotence'] == '1' else False,
    'max.in.flight.requests.per.connection' : config_obj['producer']['max.in.flight.requests.per.connection'],
    'acks'                                  : config_obj['producer']['acks'],
    'delivery.timeout.ms'                   : int(config_obj['producer']['delivery.timeout.ms']),
    'linger.ms'                             : int(config_obj['producer']['linger.ms']),
    'batch.size'                            : int(config_obj['producer']['batch.size']),
    'compression.type'                      : config_obj['producer']['compression.type']
}

producer = Producer(**producer_config)

# Initialize the UserAgent object
ua = UserAgent()
random_user_agent = ua.random

custom_headers = {
    "User-Agent": random_user_agent
}

eventHandler = EventHandler(
        kafkaProducer=producer,
        kafkaTopic=topic
    )

try:
    for event in EventSource(url_string, headers=custom_headers):
        if event.event == 'message':
            try:
                # define the message that will be sent
                wiki = {
                    'id': json.loads(event.data)['id'],
                    'title_url': json.loads(event.data)['title_url'],
                    'bot': json.loads(event.data)['bot'],
                    'user':json.loads(event.data)['user'],
                    'type': json.loads(event.data)['type'],
                }

                # send messages by json serializer
                eventHandler.on_message(
                    key = str(json.loads(event.data)['id']),
                    msg = str(event.data)
                    #json_serializer(wiki, SerializationContext(topic, MessageField.VALUE))
                    #avro_serializer(wiki, SerializationContext(topic, MessageField.VALUE))
                )
                time.sleep(1)
            except Exception as e:
                print(f"error: {e}")

# error handler, print out the message and the error info
except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Close down consumer to commit final offsets.
    print('closing...')
    eventHandler.on_close()
    print('closed')


