from confluent_kafka.admin import AdminClient, NewTopic
import configparser

# read config from the config.ini file
config_obj = configparser.ConfigParser()
config = config_obj.read('config.ini')


# topic configuration
num_partitions = int(config_obj['topic']['num_partitions'])
replication_factor = int(config_obj['topic']['replication_factor'])
topic_name = config_obj['topic']['name']

# Configuration for the AdminClient
conf = {
        'bootstrap.servers': config_obj['topic']['bootstrap.servers']
    }

# Create an AdminClient instance
admin_client = AdminClient(conf)

# delete topic if exists
# new_topics = [topic_name]
# delete_topics = admin_client.delete_topics(new_topics)
#
# for topic, future in delete_topics.items():
#     try:
#         delete_topics.result()
#         print(f"Topic '{topic}' deleted successfully")
#     except Exception as e:
#         print(f"Failed to delete topic '{topic}': {e}")

new_topics = [NewTopic(
    topic_name,
    num_partitions,
    replication_factor)
]
futures = admin_client.create_topics(new_topics, validate_only=False)

for topic, future in futures.items():
    try:
        future.result()
        print(f"Topic '{topic}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")
