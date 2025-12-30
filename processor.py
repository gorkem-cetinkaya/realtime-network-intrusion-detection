from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, split, when
from pyspark.sql.types import StructType, StringType, ArrayType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# --- CONFIGURATION ---
KAFKA_TOPIC = "network-traffic"
KAFKA_BROKER = "kafka:9093"
DATA_PATH = "/opt/spark-apps/data/KDDTrain+.txt"

# --- GLOBAL VARIABLES FOR ACCURACY ---
GLOBAL_TOTAL_RECORDS = 0
GLOBAL_TOTAL_CORRECT = 0

def process_batch(df, epoch_id):
    """
    Calculates Batch and Global Accuracy and prints the summary table.
    No database operations here to ensure maximum stability.
    """
    global GLOBAL_TOTAL_RECORDS, GLOBAL_TOTAL_CORRECT
    
    # Persist the dataframe to prevent re-computation
    df.persist()
    batch_total = df.count()
    
    if batch_total > 0:
        # Calculate correct predictions in this batch
        batch_correct = df.filter(col("is_correct") == 1).count()
        batch_accuracy = (batch_correct / batch_total) * 100
        
        # Update Global Metrics
        GLOBAL_TOTAL_RECORDS += batch_total
        GLOBAL_TOTAL_CORRECT += batch_correct
        global_accuracy = (GLOBAL_TOTAL_CORRECT / GLOBAL_TOTAL_RECORDS) * 100
        
        # --- PRINT THE REPORT ---
        print(f"\n=================================================")
        print(f"BATCH ID: {epoch_id}")
        print(f"-------------------------------------------------")
        print(f"RECORDS IN BATCH      : {batch_total}")
        print(f"BATCH ACCURACY        : %{batch_accuracy:.2f}")
        print(f"-------------------------------------------------")
        print(f"TOTAL RECORDS         : {GLOBAL_TOTAL_RECORDS}")
        print(f"GLOBAL ACCURACY       : %{global_accuracy:.2f}")
        print(f"=================================================")
        
        # Show sample rows for verification
        df.select("protocol_type", "service", "label_raw", "prediction", "is_correct").show(10, truncate=False)
    else:
        print(f"\nBatch {epoch_id} is empty.")
        
    # Unpersist to free memory
    df.unpersist()

def main():
    # Initialize Spark Session with Kafka dependency
    spark = SparkSession.builder \
        .appName("NetworkIntrusionDetection") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # --- PHASE 1: TRAINING (Static Data) ---
    print(">>> Phase 1: Training Model on KDDTrain+...")
    
    # Load static dataset for training
    static_df = spark.read.text(DATA_PATH)
    
    # Parse CSV data (Extracting specific columns for the model)
    processed_df = static_df.select(split(col("value"), ",").alias("cols")).select(
            col("cols")[0].cast("float").alias("duration"),
            col("cols")[1].alias("protocol_type"),
            col("cols")[2].alias("service"),
            col("cols")[3].alias("flag"),
            col("cols")[4].cast("float").alias("src_bytes"),
            col("cols")[5].cast("float").alias("dst_bytes"),
            col("cols")[41].alias("label_raw")
        )
    
    # Prepare training data: Convert labels 'normal' -> 0.0, others -> 1.0 (Anomaly)
    # Limiting to 10000 rows for faster training in demo
    training_data = processed_df.withColumn("label", when(col("label_raw") == "normal", 0.0).otherwise(1.0)).limit(10000)

    # Define ML Pipeline Stages
    indexer_proto = StringIndexer(inputCol="protocol_type", outputCol="protocol_ind", handleInvalid="keep")
    indexer_serv = StringIndexer(inputCol="service", outputCol="service_ind", handleInvalid="keep")
    indexer_flag = StringIndexer(inputCol="flag", outputCol="flag_ind", handleInvalid="keep")
    assembler = VectorAssembler(inputCols=["duration", "protocol_ind", "service_ind", "flag_ind", "src_bytes", "dst_bytes"], outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
    
    pipeline = Pipeline(stages=[indexer_proto, indexer_serv, indexer_flag, assembler, lr])
    
    # Train the Model
    model = pipeline.fit(training_data)
    
    # --- CALCULATE TRAINING ACCURACY (SANITY CHECK) ---
    print(">>> Evaluating Model on Training Data...")
    train_predictions = model.transform(training_data)
    correct_train = train_predictions.filter(col("label") == col("prediction")).count()
    total_train = train_predictions.count()
    
    if total_train > 0:
        training_accuracy = (correct_train / total_train) * 100
    else:
        training_accuracy = 0.0

    print(f"=================================================")
    print(f"TRAINING SET ACCURACY : %{training_accuracy:.2f}")
    print(f"TOTAL TRAINING RECORDS: {total_train}")
    print(f"=================================================")
    # ----------------------------------------------------

    print(">>> Model Trained. Starting Streaming...")

    # --- PHASE 2: STREAMING (Kafka Data) ---
    # Read from Kafka Topic
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON data coming from Kafka
    json_schema = StructType().add("csv_data", ArrayType(StringType()))
    streaming_parsed = raw_stream.select(from_json(col("value").cast("string"), json_schema).alias("data")) \
        .select(col("data.csv_data").alias("cols")).select(
            col("cols")[0].cast("float").alias("duration"),
            col("cols")[1].alias("protocol_type"),
            col("cols")[2].alias("service"),
            col("cols")[3].alias("flag"),
            col("cols")[4].cast("float").alias("src_bytes"),
            col("cols")[5].cast("float").alias("dst_bytes"),
            col("cols")[41].alias("label_raw")
        )

    # Prepare streaming data for prediction
    streaming_data = streaming_parsed.withColumn("label", when(col("label_raw") == "normal", 0.0).otherwise(1.0))
    
    # Make Predictions
    predictions = model.transform(streaming_data)
    
    # Check if prediction is correct
    processed_stream = predictions.withColumn("is_correct", when(col("label") == col("prediction"), 1).otherwise(0))

    # Output to Console (using process_batch for calculation)
    query = processed_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()