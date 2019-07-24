
entry = {'category': 'client', 'clientId': 'client_97', 'endDate': '2019-06-24', 'label': 'imprévu', 'name': 'imprevu', 'startDate': '2019-04-11', 'status': 'en_cours', 'subject': 'depenses de sante', 'event_type': 'ent'}
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
