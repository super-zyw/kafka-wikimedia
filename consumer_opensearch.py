import getopt
import json
import logging
from confluent_kafka import Consumer, KafkaException
from opensearchpy import OpenSearch
from opensearch_dsl import Search
import configparser
from helper import *
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import AvroDeserializer


# read config from the config.ini file
config_obj = configparser.ConfigParser()
config = config_obj.read('config.ini')


topic = config_obj['topic']['name']
# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {
    'bootstrap.servers'         : config_obj['topic']['bootstrap.servers'],
    'group.id'                  : config_obj['consumer']['group.id'],
    'session.timeout.ms'        : config_obj['consumer']['session.timeout.ms'],
    'auto.offset.reset'         : config_obj['consumer']['auto.offset.reset'],
    'auto.commit.interval.ms'   : config_obj['consumer']['auto.commit.interval.ms'],
    'enable.auto.offset.store'  : True if config_obj['consumer']['enable.auto.offset.store'] == 'True' else False,
    'enable.auto.commit'        : True if config_obj['consumer']['enable.auto.commit'] == 'True' else False
    #'group.remote.assignor'     : config_obj['consumer']['group.remote.assignor']
}


# schema_registry_conf = {"url": config_obj["schema_registry"]["host"]}
# schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# with open(config_obj['schema_registry']['schema_path'], 'r') as file:
#     schema_str = file.read()
# print(f"schema: {schema_str}")


# schema_registry_client = SchemaRegistryClient(schema_registry_conf)
# avro_deserializer = AvroDeserializer(
#     schema_str = schema_str,
#     schema_registry_client = schema_registry_client,
#     from_dict=lambda obj, ctx: obj
# )



# Create logger for consumer (logs will be emitted when poll() is called)
logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

# Create Consumer instance
consumer = Consumer(conf, logger=logger)
consumer.subscribe([topic], on_assign=print_assignment)


host = config_obj['consumer']['host']
port = config_obj['consumer']['port']
index_name = config_obj['consumer']['index_name']

# Create the client with SSL/TLS and hostname verification disabled.
print("create an opensearch client...")
client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True,
    use_ssl = False,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False
)

response = client.indices.exists(index = index_name) # CHECK IF THE INDEX IN OPENSEARCH

if not response:
    response = client.indices.create(index=index_name)
    print(f'creating new index name: {index_name}')
else:
    print(f'index name {index_name} already exists')
# Read messages from Kafka, print to stdout

try:
    while True:
        msg = consumer.poll(timeout=3.0)

        if msg is None or msg.value() == b"":
            # Proper message
            print('polling...')
            continue
        if msg.error():
            raise KafkaException(msg.error())

        else:
            try:
                data = msg.value()
                #data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

                response = client.index(
                    index=index_name,
                    body = data,
                )

                sys.stderr.write(
                    '%% %s [%d] at offset %d with key %s:\n'
                    % (msg.topic(), msg.partition(), msg.offset(), str(msg.key()))
                )
            except:
                print(f"error offset: {msg.offset()}, data: {data}")

            consumer.commit(msg)

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Close down consumer to commit final offsets.
    print('closing...')
    consumer.close()
    print('closed')