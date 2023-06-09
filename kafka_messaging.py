from flask import Flask, request, render_template, redirect, url_for
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


@app.route('/user1')
def page1():
    return render_template('page1.html', messages=app.config['MESSAGES'])

# route to handle user input and send it to Kafka producer
@app.route('/send_message1', methods=['POST'])
def send_message1():
    message = request.form['message']
    producer.send('my_topic', message.encode())
    return redirect(url_for('page1'))


@app.route('/user2')
def page2():
    return render_template('page2.html', messages=app.config['MESSAGES'])

@app.route('/send_message2', methods=['POST'])
def send_message2():
    message = request.form['message']
    producer.send('my_topic', message.encode())
    return redirect(url_for('page2'))


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
