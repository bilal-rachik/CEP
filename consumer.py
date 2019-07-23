from confluent_kafka import Consumer, KafkaError
from streamz import Stream
from streamz.dataframe import Random
from streamz.dataframe import DataFrame
import json
from dask.distributed import Client
from time import sleep
import random

#dask
client = Client('35.180.242.51:8786')
print('befor upload')

#streamz
source = Stream.from_kafka(['supramoteur'], {
   'bootstrap.servers': '35.180.242.51:9092',
   'group.id': 'mygroup1'
}, loop=client.loop)

#lient.upload_environment(name="CEP",'/home/bilal/anaconda3/envs/environment.zip')

def ecrire(entry):
    with  open('data.txt',"a+") as f:
        f.write("%s\r\n" % entry)
        insert_to(entry)

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