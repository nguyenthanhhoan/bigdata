import json
from kafka import KafkaProducer
import time
producer = KafkaProducer(bootstrap_servers='13.212.213.41:9092',
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

with open('/home/robusta/soft/data.txt') as f:
  objs = json.load(f)
  #print(f)
  for msg in objs:
     producer.send('test_topic', msg)
     print(msg)
     time.sleep(5)
