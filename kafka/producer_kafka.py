from confluent_kafka import Producer
import socket
import time
import io
import json

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname(),
    "batch.size": 1,
    
})

topic = 'vdt2024'

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


with open('./data/log_action.csv') as csv_file:
    idx = 0
    for line in csv_file:
        fields = line.replace('\n', '').split(',')
        data_set = {
            "student_code": int(fields[0]),
            "activity": fields[1],
            "numberOfFile": int(fields[2]),
            "timestamp": fields[3]
        }

        producer.produce(topic, key=str(idx), value=json.dumps(data_set).encode(), callback=acked)
        producer.poll(1)
        print('sent msg (key=', idx, ')')
        idx += 1
        time.sleep(1)

producer.flush()