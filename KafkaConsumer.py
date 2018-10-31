import threading
import logging
import time
import json
#https://medium.com/@mukeshkumar_46704/consume-json-messages-from-kafka-using-kafka-pythons-deserializer-859f5d39e02c

import logging
from kafka import KafkaConsumer, KafkaProducer
import mysql.connector

mydb = mysql.connector.connect(
  host="test-mysql",
  user="sundip",
  passwd="asdlkj12345",
  database="Candidate"
)

class Consumer():
    #daemon = True
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='confkafka-cp-kafka:9092',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['candidate-topic'])
        for message in consumer:

            logging.debug(message)

            #logging.log("log")
            # logging.debug(message)    

            # mycursor = mydb.cursor()

            # sql = "insert into BasicData (FirstName, LastName, Email) VALUES (%s, %s, %s)"
            # val = ("TestF", "TestL", "TEstEmail1")
            # mycursor.execute(sql, val)

            # mydb.commit()

            # logging.debug(mycursor.rowcount, "record inserted.")

c = Consumer()
c.run()