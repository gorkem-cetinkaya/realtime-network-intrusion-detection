from pymongo import MongoClient

# --- CONFIGURATION ---
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'network_data'
COLLECTION_NAME = 'raw_traffic'

try:
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # 1. Count Total Records
    count = collection.count_documents({})
    
    print("-" * 40)
    print(f"DATABASE STATUS CHECK")
    print("-" * 40)
    print(f"Database Name   : {DB_NAME}")
    print(f"Collection Name : {COLLECTION_NAME}")
    print(f"TOTAL RECORDS   : {count}")
    print("-" * 40)
    
    # 2. Show Latest Record Sample
    if count > 0:
        print("\n>>> LATEST RECORD SAMPLE:")
        # Fetch the most recently inserted document
        last_record = collection.find_one(sort=[('_id', -1)]) 
        print(last_record)
    else:
        print("\n>>> WARNING: Database appears to be empty!")

except Exception as e:
    print(f">>> Connection Error: {e}")