from dotenv import load_dotenv
import os
from pymongo import MongoClient
from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from app.main import events_collection  # import your MongoDB collection


load_dotenv()
SECERET_KEY = os.getenv("SECRET_KEY").encode()
MONGODB_URL = os.getenv("MONGODB_URL")

mongo_client = MongoClient(MONGODB_URL)
db = mongo_client.webhooks
events_collection = db.events

def ensure_indexes():
    try:
        events_collection.create_index([("status", ASCENDING)], name="idx_status")
        events_collection.create_index([("event_type", ASCENDING)], name="idx_event_type")
        events_collection.create_index([("received_at", ASCENDING)], name="idx_received_at")
        events_collection.create_index([("status", ASCENDING), ("received_at", ASCENDING)], name="idx_status_received_at")
        events_collection.create_index([("event_type", ASCENDING), ("received_at", ASCENDING)], name="idx_event_type_received_at")
        print("Indexes ensured successfully.")
    except PyMongoError as e:
        print(f"Error creating indexes: {e}")
