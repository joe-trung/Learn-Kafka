import json
import time
from kafka import KafkaProducer

# create a Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))

# loop 20 times and send JSON messages to the 'test' topic
for i in range(20):
    data = {'message': f'Hello, World! {i}', 'count': i}
    producer.send('test', value=data)
    producer.flush()
    time.sleep(1)

# close the producer connection
producer.close()