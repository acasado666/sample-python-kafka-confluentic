# Kafka Producer and Consumer with Python

This project demonstrates how to build a **Kafka producer** and **consumer** using Python with the [Confluent Kafka library](https://github.com/confluentinc/confluent-kafka-python).

## Project Overview

This sample application shows a practical example of:
- **Producer**: Sends order messages to a Kafka topic
- **Consumer**: Reads and processes messages from the topic
- **Configuration Management**: Centralized configuration for both components

## Prerequisites

Before running this project, ensure you have:

- **Python 3.7+** installed
- **Docker** and **Docker Compose** (to run Kafka locally)
- **pip** (Python package manager)

## Installation

### 1. Install Dependencies

Install the required Python packages:

At this time pip3 version is  26.1.2

```bash
pip3 install confluent-kafka
```

### 2. Start Kafka

Use Docker Compose to start a Kafka broker locally:

```bash
docker-compose up -d
```

This will start a Kafka instance on `localhost:9092`.

To verify Kafka is running:

```bash
docker ps
```

You should see the `kafka1` container running.

## Project Structure

```
sample-python-kafka/
├── app/
│   ├── config.py        # Configuration settings
│   ├── producer.py      # Kafka producer implementation
│   └── consumer.py      # Kafka consumer implementation
├── docker-compose.yml   # Docker setup for Kafka
└── README.md           # This file
```

## Configuration (config.py)

The `config.py` file contains centralized configuration for both producer and consumer:

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # Kafka broker address
TOPIC_NAME = "orders"                       # Kafka topic name

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
}

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "orders-consumer-group",    # Consumer group ID
    "auto.offset.reset": "earliest",        # Start from the beginning of the topic
}
```

### Configuration Parameters

- **bootstrap.servers**: Address of the Kafka broker
- **group.id**: Unique identifier for the consumer group (allows multiple consumers to work together)
- **auto.offset.reset**: Determines what happens when there's no initial offset - `earliest` reads from the start

## Running the Producer

The producer generates sample order messages and sends them to the Kafka topic:

```bash
cd app
python producer.py
```

### What It Does

1. Creates 10 sample order messages with:
   - Order ID
   - Customer name
   - Amount
   - Timestamp
2. Sends each message to the `orders` topic
3. Includes a callback to confirm delivery
4. Waits 1 second between messages

### Sample Output

```
Message delivered to topic=orders partition=0 offset=0
Message delivered to topic=orders partition=0 offset=1
Message delivered to topic=orders partition=0 offset=2
...
```

## Running the Consumer

The consumer listens to messages from the Kafka topic:

```bash
cd app
py consumer.py
```

### What It Does

1. Subscribes to the `orders` topic
2. Polls for messages continuously
3. Decodes and displays:
   - Message key
   - Message value (JSON)
   - Topic name
   - Partition number
   - Offset (message position)
4. Handles errors gracefully
5. Closes connection on keyboard interrupt (Ctrl+C)

### Sample Output

```
Waiting for messages...
Received message:
  Key: 0
  Value: {'order_id': 0, 'customer': 'Antonio', 'amount': 25.5, 'created_at': 1234567890.123}
  Topic: orders
  Partition: 0
  Offset: 0

Received message:
  Key: 1
  Value: {'order_id': 1, 'customer': 'Antonio', 'amount': 26.5, 'created_at': 1234567891.456}
  Topic: orders
  Partition: 0
  Offset: 1
...
```

## How to Use

### Scenario 1: Send and Receive Messages in Real-Time

1. **Terminal 1** - Start the consumer:
   ```bash
   cd app
   py consumer.py
   ```
   The consumer will wait for messages.

2. **Terminal 2** - Run the producer:
   ```bash
   cd app
   py producer.py
   ```
   Messages will be produced and immediately received by the consumer.

### Scenario 2: Replay Messages

1. Stop the consumer (Ctrl+C)
2. Run the producer again to send new messages
3. Start the consumer - it will receive all messages from the beginning (due to `auto.offset.reset: "earliest"`)

## Key Concepts

### Producer

The producer uses a **callback function** (`delivery_report`) to confirm when messages are successfully delivered:

```python
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")
```

- **Key**: Identifies the message (used for partitioning)
- **Value**: The actual message content (JSON in this case)
- **Callback**: Executed after delivery attempt

### Consumer

The consumer uses a **polling mechanism** to fetch messages:

```python
msg = consumer.poll(1.0)  # Wait up to 1 second for a message
```

- **Partitions**: Messages can be distributed across partitions for scalability
- **Offsets**: Track the position in the topic (enables message replay)
- **Consumer Group**: Multiple consumers in the same group share the load

## Stopping Services

### Stop the Consumer
Press `Ctrl+C` in the consumer terminal.

### Stop Kafka
```bash
docker-compose down
```

To remove volumes as well:
```bash
docker-compose down -v
```

## Troubleshooting

### Connection Refused Error
- Ensure Kafka is running: `docker ps`
- Verify the bootstrap server address in `config.py` matches your Kafka setup
- Wait a few seconds after starting Docker Compose for Kafka to be ready

### No Messages Received
- Ensure the producer has finished before checking the consumer
- Verify both are using the same topic name (`orders`)
- Check that the consumer group ID matches in the configuration

### Message Ordering
- Messages with the same key are guaranteed to be in order within a partition
- The producer uses `order_id` as the key, ensuring orders are processed in sequence

## Dependencies

- **confluent-kafka**: Python client for Apache Kafka
  - Installation: `pip install confluent-kafka`
  - Documentation: [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)

## Next Steps

To extend this project:

1. **Error Handling**: Add retry logic and dead-letter queues
2. **Schema Registry**: Use Avro or Protobuf for message schemas
3. **Transactions**: Implement exactly-once semantics
4. **Monitoring**: Add metrics and logging
5. **Multiple Partitions**: Configure topics with multiple partitions for better throughput
6. **Consumer Groups**: Run multiple consumers to process messages in parallel

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Python Client](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Kafka Use Cases](https://www.confluent.io/blog/use-cases-for-kafka/)

## License

This project is provided as-is for educational purposes.

