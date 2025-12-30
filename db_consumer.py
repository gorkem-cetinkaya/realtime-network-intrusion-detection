import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# --- CONFIGURATION ---
KAFKA_TOPIC = 'network-traffic'
KAFKA_BROKER = 'localhost:9092'
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'network_data'
COLLECTION_NAME = 'raw_traffic'

def start_consumer():
    # 1. Connect to MongoDB
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print(f">>> Connected to MongoDB at {MONGO_URI}")
    except Exception as e:
        print(f">>> MongoDB Connection Error: {e}")
        return

    # 2. Connect to Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f">>> Listening to topic '{KAFKA_TOPIC}' and saving to DB...")

    # 3. Process Messages
    try:
        for message in consumer:
            data = message.value
            
            # Save raw data to MongoDB
            result = collection.insert_one(data)
            
            print(f"[DB SAVED] ID: {result.inserted_id}")

    except KeyboardInterrupt:
        print("\n>>> Consumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_consumer()