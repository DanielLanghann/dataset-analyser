import os
import json
from dataset_analyzer.mongo_db_analyzer import MongoDBAnalyzer
from dotenv import load_dotenv

load_dotenv()


def main():
    """
    Demonstrate the MongoDB Analyzer service.
    """
    # Get MongoDB connection details from environment variables or use defaults
    mongo_uri = os.environ.get("MONGODB_URI")
    mongo_db = os.environ.get("MONGO_DB")

    # Create the analyzer
    print(f"Connecting to MongoDB: {mongo_uri}, Database: {mongo_db}")
    analyzer = MongoDBAnalyzer(mongo_uri, mongo_db)



if __name__ == "__main__":
    main()