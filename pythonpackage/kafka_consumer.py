from kafka import KafkaConsumer
import json

def create_consumer():
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer
def consume_messages(consumer):
    for message in consumer:
        print(f"Received message: {message.value}")

if __name__ == "__main__":
    consumer = create_consumer()
    consume_messages(consumer)