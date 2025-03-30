import os
import time
import pymongo
import statistics
from datetime import datetime

from dotenv import load_dotenv


class MongoDBDatasetLatencyAnalyzer:

    def __init__(self, connection_string, database_name, collection_name):
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = pymongo.MongoClient(connection_string)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]

    def measure_mongodb_latency(self, operations: int=100):
        """
        Measure MongoDB latency across different operations.

        Args:
            operations: Number of operations to perform for each test

        Returns:
            Dictionary with latency statistics
        """
        # Test document
        test_doc = {"timestamp": datetime.now(), "test_value": "latency_test"}

        # Store results
        results = {
            "insert": [],
            "find": [],
            "update": [],
            "delete": []
        }

        # Measure insert latency
        for i in range(operations):
            test_doc["operation_id"] = i
            start_time = time.time()
            inserted_id = self.collection.insert_one(test_doc).inserted_id
            end_time = time.time()
            results["insert"].append((end_time - start_time) * 1000)  # Convert to ms

            # Use the inserted document for subsequent tests
            doc_id = inserted_id

            # Measure find latency
            start_time = time.time()
            self.collection.find_one({"_id": doc_id})
            end_time = time.time()
            results["find"].append((end_time - start_time) * 1000)

            # Measure update latency
            start_time = time.time()
            self.collection.update_one({"_id": doc_id}, {"$set": {"updated": True}})
            end_time = time.time()
            results["update"].append((end_time - start_time) * 1000)

            # Measure delete latency
            start_time = time.time()
            self.collection.delete_one({"_id": doc_id})
            end_time = time.time()
            results["delete"].append((end_time - start_time) * 1000)

        # Calculate statistics
        stats = {}
        for operation, times in results.items():
            stats[operation] = {
                "min": min(times),
                "max": max(times),
                "avg": statistics.mean(times),
                "median": statistics.median(times),
                "p95": sorted(times)[int(operations * 0.95)],
                "p99": sorted(times)[int(operations * 0.99)],
                "samples": times
            }

        self.client.close()
        return stats



# Example usage
if __name__ == "__main__":
    load_dotenv()
    connection_string = os.getenv("MONGODB_URI")
    database_name = os.getenv("MONGO_DB")
    collection_name = "device-sensor-data"

    # Run the latency tests
    analyzer = MongoDBDatasetLatencyAnalyzer(connection_string, database_name, collection_name)
    stats = analyzer.measure_mongodb_latency()
    print(stats)

