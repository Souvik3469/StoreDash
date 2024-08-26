from pymongo import MongoClient
from app.config import settings

client = MongoClient(settings.mongodb_uri)
db = client.restaurant_monitoring
print("Connected to MongoDB Atlas")
