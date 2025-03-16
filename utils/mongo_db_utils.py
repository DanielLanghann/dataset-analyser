import logging
import os

from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_mongo_uri():
    mongo_uri = os.getenv("MONGODB_URI")
    return mongo_uri


def get_mongo_db():
    mongo_db = os.getenv("MONGO_DB")
    return mongo_db


def get_mongo_client(mongo_uri, mongo_db):
    """Connect to MongoDB and return client, db, and collection"""
    mongo_uri = mongo_uri
    mongo_db = mongo_db
    try:
        # Connect with explicit options for change streams
        client = MongoClient(
            mongo_uri,
            retryWrites=True,
            w='majority',
            readPreference='primary',
            maxPoolSize=50,
            connectTimeoutMS=5000,
            socketTimeoutMS=30000,
            serverSelectionTimeoutMS=5000
        )

        # Test connection
        server_info = client.server_info()
        logger.info(f"Connected to MongoDB version: {server_info.get('version', 'unknown')}")

        db = client[mongo_db]
        logger.info(f"Successfully connected to MongoDB database {db}")

        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


if __name__ == '__main__':
    mongo_uri = get_mongo_uri()
    mongo_db = get_mongo_db()

    print(mongo_uri)
    print(mongo_db)

    mongo_client = get_mongo_client_and_collection(mongo_uri=mongo_uri, mongo_db=mongo_db)
    print(mongo_client)
