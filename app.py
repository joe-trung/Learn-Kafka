from flask import Flask, request, render_template
from kafka import KafkaProducer, KafkaConsumer
import threading

app = Flask(__name__)

# create Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# create Kafka consumer
consumer = KafkaConsumer('my_topic', bootstrap_servers=['localhost:9092'], auto_offset_reset='latest')

# thread to constantly check for new messages from Kafka consumer
def kafka_consumer_thread():
    for message in consumer:
        # add message to a list to be rendered on the template
        app.config['MESSAGES'].append(message.value.decode())

# create a list to store messages
app.config['MESSAGES'] = []

# start Kafka consumer thread
kafka_consumer = threading.Thread(target=kafka_consumer_thread)
kafka_consumer.start()

# route for page1.html
@app.route('/')
def index():
    return render_template('page1.html')

# route to handle user input and send it to Kafka producer
@app.route('/send_message', methods=['POST'])
def send_message():
    message = request.form['message']
    # send message to Kafka producer
    producer.send('my_topic', message.encode())
    return render_template('page1.html')

# route for page2.html
@app.route('/messages')
def messages():
    return render_template('page2.html', messages=app.config['MESSAGES'])

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)