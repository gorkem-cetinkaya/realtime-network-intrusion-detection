# Real-time Network Intrusion Detection System (NIDS) 🛡️

![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.0-black)
![MongoDB](https://img.shields.io/badge/MongoDB-6.0-green)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Python](https://img.shields.io/badge/Python-3.x-yellow)

A scalable, real-time streaming analytics platform designed to detect network anomalies and cyber attacks using **Big Data** technologies. This project leverages **Apache Kafka** for data ingestion, **Apache Spark (Structured Streaming)** for real-time processing/Machine Learning, and **MongoDB** for data persistence.

The system simulates a real-world network environment where traffic is analyzed on the fly to distinguish between "Normal" traffic and "Attacks" (e.g., Neptune, Smurf).

## 🏗️ Architecture
The project follows a modern Lambda Architecture approach:

1.  **Ingestion Layer:** Python Producer reads network logs (NSL-KDD dataset) and pushes them to **Apache Kafka**.
2.  **Processing Layer:** **Spark Structured Streaming** consumes data from Kafka. It uses a **Logistic Regression** model (trained on static data) to predict anomalies in real-time.
3.  **Storage Layer:** Raw traffic and results are stored in **MongoDB** via a separate consumer.
4.  **Orchestration:** All services are containerized using **Docker Compose**.

## 🚀 Key Features
* **Real-time Simulation:** Simulates network traffic flow with adjustable delays.
* **Hybrid Analysis:** Combines batch training (Phase 1) with stream processing (Phase 2).
* **Live Metrics:** Calculates and displays "Batch Accuracy" and "Global Accuracy" dynamically.
* **Scalable:** Decoupled architecture allowing independent scaling of producers, consumers, and processors.
* **Data Integrity:** Validates and archives data in a NoSQL database.

## 📂 Project Structure

```text
network-intrusion-project/
├── data/
│   ├── KDDTrain+.txt        # Dataset used for Model Training
│   └── KDDTest+.txt         # Dataset used for Streaming Simulation
├── docker-compose.yml       # Infrastructure (Zookeeper, Kafka, Spark, Mongo)
├── processor.py             # Spark Streaming & ML Logic (The Brain)
├── producer.py              # Kafka Producer (The Simulator)
├── db_consumer.py           # MongoDB Consumer (The Archiver)
├── verify_mongo.py          # Script to verify database records
├── requirements.txt         # Local python dependencies
└── README.md                # Project Documentation
```

## ⚙️ Installation & Setup

## Prerequisites

Docker & Docker Compose installed.

Python 3.x installed (for local producer/consumer scripts).

1. Clone the Repository

```bash
git clone [https://github.com/gorkem-cetinkaya/realtime-network-intrusion-detection.git](https://github.com/gorkem-cetinkaya/realtime-network-intrusion-detection.git)
cd realtime-network-intrusion-detection
```

2. Start the Infrastructure (Docker)

This command starts Zookeeper, Kafka, Spark Master/Worker, and MongoDB.

```bash
docker-compose up -d
```

3. Install Local Dependencies

For the producer and consumer scripts running on your host machine:

```bash
pip install kafka-python pymongo
```

4. Run the Spark Processor (The Core)

This step submits the Spark job to the cluster. It will first train the model and then start listening for the stream. (Note: The command includes the necessary Kafka SQL packages)

```bash
docker exec -u 0 -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/processor.py
```

Wait until you see the message: >>> Model Trained. Starting Streaming...

5. Start Data Streaming (Producer)

Open a new terminal window. This script reads the test dataset and sends it to Kafka.

```bash
python producer.py
```

6. Start Database Consumer (Optional)

Open another terminal window. This script saves the raw traffic to MongoDB.

```bash
python db_consumer.py
```

## 📊 Results & Performance
The system calculates accuracy in real-time. Below is a snapshot of the performance:

Training Accuracy: ~95.0% (Calculated on KDDTrain+)

Global Streaming Accuracy: ~82.5% (Calculated on live KDDTest+ stream)

## 📄 Documentation
For a deep dive into the theoretical background, sequence diagrams, and detailed analysis, please refer to the Project Report (PDF).

## 🤝 Contributing
Contributions, issues, and feature requests are welcome!

## 📜 License
This project is open-source and available under the MIT License.
