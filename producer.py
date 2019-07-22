from confluent_kafka import Producer
import json
import logging
import pandas as pd
logging.basicConfig(level=logging.DEBUG)
p = Producer({'bootstrap.servers': '35.180.242.51:9092'})
file = pd.read_csv("data/small_events.csv", sep=';')
file['nametype'] = "event"
listEvents = json.loads(file.to_json(orient='records'))
def delivery_report(err, msg):
  """ Called once for each message produced to indicate delivery result.
      Triggered by poll() or flush(). """
  if err is not None:
      print('Message delivery failed: {}'.format(err))
  else:
      print('Message delivered to "{}" [{}]'.format(msg.topic(), msg.partition()))
for data in listEvents:
  # Trigger any available delivery report callbacks from previous produce() calls
  p.poll(0)
  # Asynchronously produce a message, the delivery report callback
  # will be triggered from poll() above, or flush() below, when the message has
  # been successfully delivered or failed permanently.
  print(f'sending {json.dumps(data)}')
  p.produce('supramoteur', json.dumps(data).encode('utf-8'), callback=delivery_report)
# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
