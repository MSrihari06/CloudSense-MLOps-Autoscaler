import json
import time
from confluent_kafka import Producer

# Connect to local Redpanda broker
conf = {'bootstrap.servers': 'localhost:19092'}
producer = Producer(conf)

topic = 'social-buzz'

# Sample social buzz payload
sample_data = {
    "timestamp": time.time(),
    "platform": "reddit",
    "text": "Traffic spike expected on cloud services due to launch event!",
    "mention_count": 42
}

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message successfully delivered to topic '{msg.topic()}' [partition {msg.partition()}]")

# Produce payload
producer.produce(topic, key="test_key", value=json.dumps(sample_data), callback=delivery_report)
producer.flush()