import logging
import json
import os

import pymongo
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()


class MongoDBCollectionSizeAnalyzer:
    def __init__(self, uri=None, database=None, collection=None, log_level=logging.INFO):
        """
        Initialize the SchemaAnalyzer with connection parameters.

        Args:
            uri (str): MongoDB connection URI
            database (str): Database name
            collection (str): Collection name
            log_level: Logging level
        """
        self.uri = uri
        self.database = database
        self.collection = collection
        # Setup logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("MongoDBCollectionSizeAnalyzer")

    def analyze_collection_size(self):
        """
        Analyze the collection size:
        - total size in bytes
        - storage size in bytes
        - average document size in bytes
        - document count
        - wired tiger stats
        Save the results in a json file with the given database and collection name as part of the filename.
        """
        self.logger.info(f"Analyzing size of collection {self.collection} in database {self.database}")

        # Connect to MongoDB
        try:
            client = pymongo.MongoClient(self.uri)
            db = client[self.database]
            coll = db[self.collection]

            # Get collection stats
            stats = db.command("collStats", self.collection)

            # Extract relevant information
            size_info = {
                "database": self.database,
                "collection": self.collection,
                "timestamp": datetime.now().isoformat(),
                "document_count": stats.get("count", 0),
                "total_size_bytes": stats.get("size", 0),
                "storage_size_bytes": stats.get("storageSize", 0),
                "avg_document_size_bytes": stats.get("avgObjSize", 0),
                "index_size_bytes": stats.get("totalIndexSize", 0),
                "wired_tiger": stats.get("wiredTiger", {})
            }

            # Save results to file
            filename = f"{self.database}_{self.collection}_size_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w') as f:
                json.dump(size_info, f, indent=4)

            self.logger.info(f"Size analysis completed and saved to {filename}")
            return size_info

        except Exception as e:
            self.logger.error(f"Error analyzing collection size: {str(e)}")
            raise
        finally:
            if 'client' in locals():
                client.close()
                self.logger.debug("MongoDB connection closed")


if __name__ == "__main__":
    # Example usage
    uri = os.getenv('MONGODB_URI')
    database = os.getenv('MONGO_DB')
    collection = "device-playout"

    size_analyzer = MongoDBCollectionSizeAnalyzer(uri=uri, database=database, collection=collection)
    results = size_analyzer.analyze_collection_size()
    print(
        f"Collection has {results['document_count']} documents with average size of {results['avg_document_size_bytes']} bytes")