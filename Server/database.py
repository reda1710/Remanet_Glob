from pymongo import MongoClient
import logging
from config import get_settings

# Set up proper logging
logger = logging.getLogger(__name__)

class Database:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance.client = None
            cls._instance.db = None
        return cls._instance
    
    def initialize(self):
        settings = get_settings()
        try:
            self.client = MongoClient(settings.MONGODB_URI)
            self.db = self.client[settings.MONGODB_DB_NAME]
            logger.info("MongoDB connection successful")
            return True
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            return False
    
    def get_collection(self, collection_name):
        if self.db is not None:
            return self.db[collection_name]
        return None
    
    def close(self):
        if self.client is not None:
            self.client.close()
            logger.info("MongoDB connection closed")

db = Database()
