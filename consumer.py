from kafka import KafkaConsumer

# create a Kafka consumer
consumer = KafkaConsumer('test', bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('ascii')))

# continuously poll for new messages and print them
for message in consumer:
    data = message.value
    print(data['message'], data['count'])
