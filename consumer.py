from kafka import KafkaConsumer
from json import loads

broker_url = 'localhost:9092'
topic_name = 'telemetry_csv_3'

consumer = KafkaConsumer(topic_name,
    bootstrap_servers=broker_url,
    auto_offset_reset='latest')

print('Start receiving data.')

for message in consumer:
    print('Catched data: %s' % message.value)