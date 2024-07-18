import pandas as pd
from confluent_kafka import Producer
import socket
import threading
import time

# Đọc dữ liệu từ tệp CSV
data = pd.read_csv('./data/log_action.csv')
data.columns = ['student_code', 'activity', 'numberOfFile', 'timestamp']

# Hàm để gửi tin nhắn
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Cấu hình Kafka Producer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname(),
    "batch.size": 1,
}

# 2 thread for 2 producer

def producer(ip, np):
    producer = Producer(conf)

    # Gửi dữ liệu tới topic "topic2"
    for index, row in data.iterrows():
        if index % np != ip:
            continue
        producer.produce('topic2', key=str(index), value=row.to_json(), callback=delivery_report)
        producer.poll(0)
        print(f"Producer {ip + 1} Sent message (key={index})")
        time.sleep(1)

    producer.flush()

if __name__ == '__main__':
    # Tạo 2 thread để gửi dữ liệu
    thread1 = threading.Thread(target=producer, args=(0, 2))
    thread2 = threading.Thread(target=producer, args=(1, 2))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()
