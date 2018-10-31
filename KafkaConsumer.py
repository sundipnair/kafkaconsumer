import threading
import logging
import time
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
import mysql.connector

mydb = mysql.connector.connect(
  host=" 10.0.80.240:3306",
  user="sundip",
  passwd="asdlkj12345",
  database="Candidate"
)

# class Producer(threading.Thread):
#     daemon = True
#     def run(self):
#         producer = KafkaProducer(bootstrap_servers='confkafka-cp-kafka:9092',
#                                  value_serializer=lambda v: json.dumps(v).encode('utf-8'))

#         producer.send('candidate-topic', {"Id":0,"FirstName":"Kafka1","LastName":"Prod1","Email":"prod1@kafka.com"})

#         # while True:
#         #     producer.send('my-topic', {"dataObjectID": "test1"})
#         #     producer.send('my-topic', {"dataObjectID": "test2"})
#         #     time.sleep(1)


# prod = Producer()
# prod.run()


class Consumer(threading.Thread):
    daemon = True
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='confkafka-cp-kafka:9092',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['candidate-topic'])
        for message in consumer:
            print (message)
            logging.debug(message)    

            mycursor = mydb.cursor()

            sql = "insert into BasicData (FirstName, LastName, Email) VALUES (%s, %s, %s)"
            val = ("TestF", "TestL", "TEstEmail")
            mycursor.execute(sql, val)

            mydb.commit()

            print(mycursor.rowcount, "record inserted.")

c = Consumer()
c.run()