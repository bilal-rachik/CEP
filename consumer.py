from confluent_kafka import Consumer, KafkaError
import logging
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.DEBUG)

c = Consumer({
   'bootstrap.servers': '35.180.242.51:9092',
   'group.id': 'mygroup',
   'auto.offset.reset': 'earliest'
})

cluster = Cluster(['35.180.242.51'])
session = cluster.connect('supramoteur')

c.subscribe(['supramoteur'])

while True:
   msg = c.poll(1.0)
   if msg is None:
       continue
   if msg.error():
       print("Consumer error: {}".format(msg.error()))
       continue

   print('Received message: {}'.format(msg.value().decode('utf-8')))
   rows = session.execute('SELECT * FROM test')
   for user_row in rows:
       print('user ==> ', user_row)

c.close()