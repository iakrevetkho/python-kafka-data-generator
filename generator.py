import json
from time import sleep
from json import dumps
from kafka import KafkaProducer

broker_url = 'localhost:9092'
topic_name = 'test'

producer = KafkaProducer(bootstrap_servers=broker_url)

print('Start generating data.')


for e in range(1000):
    data = '{"number":%d}' % (e)
    producer.send('test', data.encode())

    sleep(1)
    print('Send message: %s' % data)