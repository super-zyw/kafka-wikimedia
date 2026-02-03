from quixstreams import Application
import configparser
import json
import quixstreams.dataframe.windows.aggregations as agg


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

# Define the Kafka topics
input_topic = app.topic(config_obj['topic']['name'], value_deserializer='json')
output_topic = app.topic("wikimedia-type-count", value_serializer='json')

# Create a Streaming DataFrame connected to the input Kafka topic
sdf = app.dataframe(topic=input_topic)

# Filter values above the threshold
sdf_agg = sdf.group_by('type')\
         .tumbling_window(duration_ms=10000)\
         .agg(value=agg.Count())\
         .current()

# Produce alerts to the output topic
sdf_agg.to_topic(output_topic)
app.run()
