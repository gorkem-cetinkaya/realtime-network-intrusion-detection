import time
import json
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'network-traffic'
# We use the TEST dataset for streaming to avoid data leakage
DATA_FILE = '../data/KDDTest+.txt'  
SLEEP_TIME = 0.1  # Simulate real-time delay

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print(f">>> Connected to Kafka Broker at {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f">>> ERROR: Could not connect to Kafka: {e}")
        return None

def stream_data():
    producer = create_producer()
    if not producer: return

    print(f">>> Starting data stream from {DATA_FILE}...")
    
    try:
        with open(DATA_FILE, 'r') as file:
            for line in file:
                # Prepare data
                data = line.strip().split(',')
                message = {'csv_data': data}

                # Send to Kafka
                producer.send(KAFKA_TOPIC, value=message)
                
                # Log to console
                print(f"Sent Message: {data[0:3]}...") 
                time.sleep(SLEEP_TIME)
                
    except FileNotFoundError:
        print(f">>> ERROR: File {DATA_FILE} not found.")
    except KeyboardInterrupt:
        print("\n>>> Streaming stopped by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    stream_data()