from cassandra.cluster import Cluster
from streamz import Stream
import json
from dask.distributed import Client
from time import sleep
import random
client = Client('35.180.242.51:8786')
cluster = Cluster(['35.180.227.49'])
session = cluster.connect('supramoteur')
source = Stream.from_kafka(['supramoteur'], {
  'bootstrap.servers': '35.180.242.51:9092',
  'group.id': 'mygroup'
}, loop=client.loop)
def save_data(entry):
   session.execute("""
   INSERT INTO client2 ( clientid,nametype,create_date, category,enddate,label,name,startdate,status,subject)
   VALUES (%s, %s, now(), %s, %s, %s, %s, %s, %s, %s)
   """,        (str(entry['clientId']),
                str(entry['nametype']),
                str(entry['category']),
                str(entry['endDate']),
                str(entry['label']),
                str(entry['name']),
                str(entry['startDate']),
                str(entry['status']),
                str(entry['subject'])
                ))
def print_me(x):
   print(f"Result => {x}")
def aggregate(acc, df):
   return acc+1
futures = source\
   .scatter()\
   .map(json.loads)\
   .gather()\
   .sink(save_data)
def do_poll():
   msg = source.consumer.poll(0.1)
   if msg and msg.value() and msg.error() is None:
       return msg.value()
print('source.loop', source.loop)
while True :
   sleep(random.random() / 10)
   source.start()
