from confluent_kafka import Consumer, KafkaError
from streamz import Stream
from streamz.dataframe import Random
from streamz.dataframe import DataFrame
import json
from dask.distributed import Client
from time import sleep
import random
from time import time

#dask
client = Client('35.180.242.51:8786')
import os
client.run(lambda: os.system("pip install cassandra-driver"))
print('befor upload')

#streamz
source = Stream.from_kafka(['supramoteur'], {
   'bootstrap.servers': '35.180.242.51:9092',
   'group.id': 'mygroup1'
}, loop=client.loop)

def time_final(entry):
    tt=time()-entry['time']
    return tt

def ecrire(entry):
    entry['time_before']=time()
    with  open('data.txt',"a+") as f:
        f.write("%s\r\n" % entry)
        insert_to(entry)
    entry['time_after']=round(-entry['time'],4)
    return entry


sessionmap = {}
def insert_to(entry):

    from cassandra.cluster import Cluster
    cluster_cassandra = Cluster(['35.180.227.49'])
    session = cluster_cassandra.connect('supramoteur')
    session.execute(
                    """
        INSERT INTO supramoteur.client2 (clientid, nametype,create_date,category,enddate,label,name,startdate,status,subject)
        VALUES (%s,%s,now(),%s,%s,%s,%s,%s,%s,%s)
        """, (str(entry['clientId']), entry['event_type'], entry['category'], entry['endDate'], entry['label'], entry['name'],
              entry['startDate'], entry['status']
                          , entry['subject']))

print('Starting...')
futures = source\
    .scatter()\
    .map(json.loads)\
    .map(ecrire)\
    .sink(print)

print('source.loop', source.loop)
while True :
    sleep(random.random() / 10)
    source.start()