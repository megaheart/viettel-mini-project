from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import time
import threading

# Cấu hình Kafka Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

def consume_loop(ip, topics):
    consumer = Consumer(conf)
    try:
        consumer.subscribe(topics)
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n")
                elif msg.error():
                    print(f"Error: {msg.error()}")
            else:
                # In this simple example, we're just printing the message
                print(f"Consumer {ip + 1} Received message: {msg.value().decode('utf-8')}")
            time.sleep(1)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

# 2 thread for 2 consumer
if __name__ == '__main__':
    topics = ['topic2']
    thread1 = threading.Thread(target=consume_loop, args=(0, topics))
    thread2 = threading.Thread(target=consume_loop, args=(1, topics))
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
