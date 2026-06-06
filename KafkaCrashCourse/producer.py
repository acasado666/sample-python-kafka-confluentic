import json
import uuid

from confluent_kafka import Producer

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered {msg.value().decode('utf-8')} to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        print(f"✅ Delivered to {msg.topic()}: partition {msg.partition()} : at offset {msg.offset()}")
        print(dir(msg)) # to see all values that we can access

# this is a json object

# order = {
#     "order_id": str(uuid.uuid4()),
#     "customer": random.choice(customers),
#     "amount": 25.50 + i,
#     "quantity": random.randint(1, 5),
#     "created_at": time.time(),
# }
#
# value = json.dumps(order).encode("utf-8")
# producer.produce(
#     topic=TOPIC_NAME,
#     key=str(order["order_id"]),
#     value=json.dumps(order),
#     callback=delivery_report,
# )

order = {
    'order_id': str(uuid.uuid4()),
    'user': 'antonio',
    'item': 'pizza',
    'quantity': 2,
}

value = json.dumps(order).encode('utf-8')

producer.produce('orders', value, callback = delivery_report)

producer.flush()