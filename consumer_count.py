"""
    run this consumer in a new consumer group
"""



from quixstreams import Application
import configparser
import json
import quixstreams.dataframe.windows.aggregations as agg
#from quixstreams.models import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer

# read config from the config.ini file
config_obj = configparser.ConfigParser()
config = config_obj.read('config/config.ini')


# Define an application that will connect to Kafka
app = Application(
    broker_address=config_obj['topic']['bootstrap.servers'],  # Kafka broker address
    consumer_group=config_obj['consumer.streaming.process']['group.id']
)


sr = SchemaRegistryClient({"url": config_obj["schema_registry"]["host"]})
with open(config_obj['schema_registry']['schema_path'], 'r') as file:
    schema_str = file.read()
print(f"schema: {schema_str}")
json_deserializer = JSONDeserializer(schema_str)


def decode(v):
    return json_deserializer(
        v,
        SerializationContext(config_obj['topic']['name'], MessageField.VALUE)
    )


# Define the Kafka topics
input_topic = app.topic(config_obj['topic']['name'], value_deserializer="bytes")
output_topic = app.topic("wikimedia-type-count")

# Create a Streaming DataFrame connected to the input Kafka topic
sdf = app.dataframe(topic=input_topic)
sdf = sdf.apply(decode)

# Filter values above the threshold
sdf_agg = sdf.group_by('type')\
         .tumbling_window(duration_ms=10000)\
         .agg(value=agg.Count())\
         .current()

# Produce alerts to the output topic
sdf_agg.to_topic(output_topic)
app.run()
