import json
from kafka import KafkaProducer

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    return producer
def send_message(producer, topic, message):
    producer.send(topic, message)
    producer.flush()

if __name__ == "__main__":
    producer = create_producer()
    message = {"name": "John Doe", "age": 30}
    send_message(producer, 'test-topic', message)