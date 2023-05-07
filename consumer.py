from kafka import KafkaConsumer
import json
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base

engine = create_engine('sqlite:///transactions.db')
Session = sessionmaker(bind=engine)

Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    amount = Column(Float)


# create a Kafka consumer
consumer = KafkaConsumer('test', bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('ascii')))

# continuously poll for new messages and print them
for message in consumer:
    data = message.value
    print(data['name'], data['amount'])
    transaction = Transaction(name=data['name'], amount=data['amount'])
    session = Session()
    session.add(transaction)
    session.commit()

# import sqlite3
#
# # Create a new database file
# conn = sqlite3.connect('transactions.db')
#
# # Create a cursor object to execute SQL commands
# cursor = conn.cursor()
#
# # Create a new table
# cursor.execute('''CREATE TABLE users
#                   (id INTEGER PRIMARY KEY, name TEXT, amount TEXT)''')
#
# # Close the connection
# conn.close()
