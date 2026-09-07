import json
import random
import time
from confluent_kafka import Producer
from faker import Faker
import os
from dotenv import load_dotenv

load_dotenv() # Loads the .env file

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:19092')
INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'social-buzz')

fake = Faker()

# Kafka broker connection
# Kafka broker connection
conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(conf)
topic = INPUT_TOPIC

# Keywords and sentiment contexts
KEYWORDS = ["cloud", "server crash", "launch event", "going viral", "promo deal", "downtime"]
PLATFORMS = ["reddit", "x_twitter", "news_rss"]

def generate_social_post(is_spike=False):
    """Generates synthetic social media post data."""
    keyword = random.choice(KEYWORDS)
    
    # Simulate high mention velocity during a "spike" event
    mention_count = random.randint(150, 500) if is_spike else random.randint(1, 20)
    
    if is_spike:
        text = f"CRITICAL: Massive interest in {keyword}! Everyone is accessing the portal right now! {fake.sentence()}"
    else:
        text = f"Just checking out the {keyword} updates today. {fake.sentence()}"

    return {
        "timestamp": time.time(),
        "platform": random.choice(PLATFORMS),
        "keyword": keyword,
        "text": text,
        "mention_velocity_per_min": mention_count,
        "is_spike_scenario": is_spike
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        payload = json.loads(msg.value().decode('utf-8'))
        spike_flag = "🔥 SPIKE" if payload["is_spike_scenario"] else "🟢 NORMAL"
        print(f"[{spike_flag}] Delivered to {msg.topic()} | Velocity: {payload['mention_velocity_per_min']} mentions/min")

print("🚀 Starting Social Buzz Producer... Press Ctrl+C to stop.")

try:
    step = 0
    while True:
        # --- THE SPIKE TRIGGER ---
        # Set this to True to force the massive social media viral event
        is_spike = False
        actual_server_requests = 500
        
        post_data = generate_social_post(is_spike=is_spike)
        
        producer.produce(
            topic,
            key=post_data["platform"],
            value=json.dumps(post_data),
            callback=delivery_report
        )
        producer.poll(0)
        
        step += 1
        time.sleep(2)  # Stream an event every 2 seconds

except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.flush()