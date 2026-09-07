import json
import time
import pandas as pd
from confluent_kafka import Consumer

import os
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:19092')
INPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'analyzed-buzz') 
WINDOW_SIZE_SECONDS = 10

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'feature-engineer-group',
    'auto.offset.reset': 'earliest'
}

def main():
    consumer = Consumer(consumer_conf)
    consumer.subscribe([INPUT_TOPIC])
    print(f"Listening to '{INPUT_TOPIC}' for Feature Engineering... (Press Ctrl+C to stop)")
    
    buffer = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if buffer:
                    df = pd.DataFrame(buffer)
                    current_time = time.time()
                    total_velocity = df["mentions_in_batch"].sum() if "mentions_in_batch" in df.columns else len(df)
                    
                    # Extract the server requests we passed through from the producer
                    actual_requests = df["actual_server_requests"].iloc[-1] if "actual_server_requests" in df.columns else 500
                    avg_sentiment = df["sentiment_score"].mean() if "sentiment_score" in df.columns else 0.0

                    feature_row = {
                        "timestamp": current_time,
                        "actual_server_requests": actual_requests,
                        "total_mention_velocity": total_velocity,
                        "avg_sentiment_score": avg_sentiment
                    }

                    feature_df = pd.DataFrame([feature_row])
                    feature_df.to_csv("training_features.csv", mode='a', header=not pd.io.common.file_exists("training_features.csv"), index=False)
                    
                    print(f"[{time.strftime('%H:%M:%S')}] Window processed | Velocity: {total_velocity} | Actual Requests: {actual_requests}")
                    buffer = []
                continue

            if msg.error():
                continue

            record = json.loads(msg.value().decode('utf-8'))
            buffer.append(record)

    except KeyboardInterrupt:
        print("Stopping Feature Engineer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()