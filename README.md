# Kafka for Wikimedia data streaming


This repo demonstrates the Kafka producer/consumer/streaming to process the Wikimedia data. The data will be ingested into OpenSearch.

The existing code demonstrates the raw data. However, the schema can be avro or json, depending on the needs. And the corresponding serializer/deserializer can be imported.

Needed libraries: `confluent-kalfa-python` and `quixstreams`

### config.ini
- provides a central configuration file to configure the resources
### producer.py
- kafka producer

### consumer_opensearch.py
- kafka consumer, ingests the data to `Opensearch`

### consumer_count.py 
- listen to the producer, aggregate the results and count the data by `type` using `tumbling_window`, publish the results to a new topic

### consumer_monitor.py
- listen to the topic published by the `consumer_count.py`, and visualize the counts
- [visualization.png]