from confluent_kafka import Consumer
import socket

consumer = Consumer({
    'bootstrap.servers': 'localhost:9093',
    'group.id': socket.gethostname(),
    'auto.offset.reset': 'earliest'
})

topic = 'vdt2024'
consumer.subscribe([topic])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))
    print('key:', msg.key().decode('utf-8'))

consumer.close()