import logging
import sys

class EventHandler:
    def __init__(self, kafkaProducer, kafkaTopic):
        self.kafkaProducer = kafkaProducer
        self.kafkaTopic = kafkaTopic

    def on_open(self):
        pass

    def on_close(self):
        self.kafkaProducer.flush()


    def on_message(self, msg, key):
        self.kafkaProducer.produce(
            topic=self.kafkaTopic,
            key=key,
            value=msg,
            callback=delivery_callback
        )


def delivery_callback(err, msg):
    sys.stderr.write('%% Message delivered to %s, partition [%d], key [%s], value [%s], offset [%d]\n' % (msg.topic(), msg.partition(), msg.key(), msg.value(), msg.offset()))

