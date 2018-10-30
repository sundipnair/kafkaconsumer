import threading
import logging
import time
import json
from kafka import KafkaConsumer, KafkaProducer

class Producer(threading.Thread):
    daemon = True
    def run(self):
        producer = KafkaProducer(bootstrap_servers='confkafka-cp-kafka:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        producer.send('candidate-topic', {"Id":0,"FirstName":"Kafka1","LastName":"Prod1","Email":"prod1@kafka.com"})

        # while True:
        #     producer.send('my-topic', {"dataObjectID": "test1"})
        #     producer.send('my-topic', {"dataObjectID": "test2"})
        #     time.sleep(1)


prod = Producer()
prod.run()