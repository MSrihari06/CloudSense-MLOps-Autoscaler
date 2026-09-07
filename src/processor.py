import json
from confluent_kafka import Consumer, Producer
import polars as pl
from transformers import pipeline

import os
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:19092')
INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'social-buzz')
OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'analyzed-buzz')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 5))
POLL_TIMEOUT = float(os.getenv('POLL_TIMEOUT', 1.0))

print("Loading RoBERTa Sentiment Model... (This takes a moment)")
sentiment_analyzer = pipeline(
    "sentiment-analysis", 
    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
    tokenizer="cardiffnlp/twitter-roberta-base-sentiment-latest"
)
print("Model loaded successfully! 🚀")

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'ml-processing-group',
    'auto.offset.reset': 'earliest'
}
producer_conf = {'bootstrap.servers': KAFKA_BROKER}

def process_batch(batch_data):
    if not batch_data:
        return None
    texts = [record["text"] for record in batch_data]
    sentiments = sentiment_analyzer(texts)
    for i in range(len(batch_data)):
        batch_data[i]["sentiment_label"] = sentiments[i]["label"]
        batch_data[i]["sentiment_score"] = sentiments[i]["score"]
    
    df = pl.DataFrame(batch_data)
    df = df.with_columns([
        pl.col("text").str.to_lowercase()
                      .str.replace_all(r"http\S+", "")
                      .str.replace_all(r"[^\w\s]", "")
                      .str.strip_chars()
                      .alias("cleaned_text")
    ])
    
    summary_df = df.group_by("platform").agg([
        pl.len().alias("mentions_in_batch"),
        pl.col("sentiment_label").mode().first().alias("majority_vibe")
    ])
    print("\n--- AI Analysis Complete ---")
    print(summary_df)
    return df

def main():
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    consumer.subscribe([INPUT_TOPIC])
    print(f"Listening to '{INPUT_TOPIC}' for NLP Processing... (Press Ctrl+C to stop)")
    batch = []
    
    try:
        while True:
            msg = consumer.poll(POLL_TIMEOUT)
            if msg is None:
                continue
            if msg.error():
                continue
            
            record = json.loads(msg.value().decode('utf-8'))
            batch.append(record)
            
            if len(batch) >= BATCH_SIZE:
                processed_df = process_batch(batch)
                if processed_df is not None:
                    for row in processed_df.to_dicts():
                        producer.produce(OUTPUT_TOPIC, key=row["platform"], value=json.dumps(row))
                    producer.flush()
                batch = []
                
    except KeyboardInterrupt:
        print("Stopping AI processor...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()