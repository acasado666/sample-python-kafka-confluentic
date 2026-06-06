import json

from confluent_kafka import Consumer

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracker',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

consumer.subscribe(['orders'])

def process_pizza():return "🍕"
def process_pasta():return "🍝"
def process_salad():return "🥗"

print('🟢 Consumer is running and subscribed to orders topic')

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        decoded_value = msg.value().decode('utf-8')
        order = json.loads(decoded_value) # this a dictionary json

        print(f"Received message order with key details:")
        print(f"  Value: {order}")
        print(f"  Topic: {msg.topic()}")
        print(f"  Partition: {msg.partition()}")
        print(f"  Offset: {msg.offset()}")

        print(f" Received order from customer:  {order.get('customer', 'N/A')}")

        handlers = {
            'pizza': process_pizza,
            'pasta': process_pasta,
            'salad': process_salad
        }
        food = handlers.get(order.get('item').lower(), lambda: "Unknown")()

        print(f"📦 Received from customer 🧑‍🦱 {order['customer']} an order 📜: {order['quantity']} x {food} {order['item']} for a total of 💶{order['amount']}")

except KeyboardInterrupt:
     print("\n🔴 ?? Stopping consumer...")
finally:
    consumer.close()