import json
import time
from kafka import KafkaProducer

# create a Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))

# loop 20 times and send JSON messages to the 'test' topic
for _ in range(10):
    data = {'name': 'Alice', 'amount': '100.00'}
    producer.send('test', value=data)
    producer.flush()
    time.sleep(1)

# close the producer connection
producer.close()