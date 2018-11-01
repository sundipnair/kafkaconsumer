#https://medium.com/@mukeshkumar_46704/consume-json-messages-from-kafka-using-kafka-pythons-deserializer-859f5d39e02c

import threading
import logging
import time
import json
from kafka import KafkaConsumer, KafkaProducer
import mysql.connector

mydb = mysql.connector.connect(
  host="test-mysql",
  user="sundip",
  passwd="asdlkj12345",
  database="Candidate"
)

class Consumer(threading.Thread):
    daemon = True
    def run(self):

      logging.info("test warning")

      consumer = KafkaConsumer(bootstrap_servers='confkafka-cp-kafka:9092',
                                auto_offset_reset='earliest',
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')))
      consumer.subscribe(['candidate-topic'])
      for message in consumer:
        # logging.info(message.value)    

        for item in message.value.items():
          logging.info(item)

        # logging.info(message.value.get("FirstName", "Default First Name"))
        # logging.info(message.value.get("LastName", "Default Last Name"))
        # logging.info(message.value.get("Email", "Default Email"))

        # mycursor = mydb.cursor()

        # sql = "insert into BasicData (FirstName, LastName, Email) VALUES (%s, %s, %s)"
        # val = ("TestF", "TestL", "TEstEmail1")
        # mycursor.execute(sql, val)

        # mydb.commit()

        # logging.info(mycursor.rowcount, "record inserted.")
      logging.info("thread finish")

def main():
    threads = [
        Consumer()
    ]
    for t in threads:
        t.start()
    time.sleep(1)
if __name__ == "__main__":
  logging.basicConfig(
      format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
              '%(levelname)s:%(process)d:%(message)s',
      level=logging.INFO
  )
  main()

logging.info("app finish")
