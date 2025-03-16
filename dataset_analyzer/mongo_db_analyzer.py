import pymongo
import datetime

from collections import Counter
import logging
from typing import Dict, Any, Tuple
import json
from pymongo.collection import Collection
from bson import json_util
import time
from utils.mongo_db_utils import get_mongo_client


class MongoDBAnalyzer:
    """Service to analyze MongoDB collections and extract key properties."""

    def __init__(self, mongodb_uri: str, db_name: str, collection_name: str, log_level=logging.INFO):
        """
        Initialize the MongoDB analyzer service.

        Args:
            mongodb_uri: MongoDB connection URI
            db_name: Database name to analyze
            log_level: Logging level
        """
        self.client = get_mongo_client(mongo_uri=mongodb_uri, mongo_db=db_name)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

        # Setup logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("MongoDBAnalyzer")

        # Store profiler status to restore later
        self.profiler_was_enabled = False

    def analyze_collection(self, sample_size: int = 1000,
                           profiling_duration: int = 3600) -> Dict[str, Any]:
        """
            Analyze a MongoDB collection and extract key properties.

            Args:
                collection_name: Name of the collection to analyze
                sample_size: Number of documents to sample
                profiling_duration: Duration in seconds to collect profiling data

            Returns:
             Dictionary containing analysis results
        """
        collection_name = self.collection.name
        self.logger.info(f"Starting analysis of collection: {collection_name}")

        results["data_structure"] = self._analyze_data_structure(self.collection, sample_size)

    def _analyze_data_structure(self, collection: Collection, sample_size: int) -> Dict:
        """Analyze the data structure of the collection."""
        self.logger.info("Analyzing data structure")

        # Get a sample of documents
        sample_docs = list(collection.aggregate([{"$sample": {"size": sample_size}}]))

        if not sample_docs:
            self.logger.warning(f"No documents found in collection: {collection}")
            return {"fields": {}, "document_count": 0}

        # Extract schema information
        fields = {}
        for doc in sample_docs:
            self._extract_field_info(doc, fields)

        for field_name, field_info in fields.items():
            if "types" in field_info:
                field_info["type_distribution"] = {
                    t: count / len(sample_docs) * 100
                    for t, count in field_info["types"].items()
                }
                field_info["primary_type"] = max(
                    field_info["types"].items(),
                    key=lambda x: x[1]
                )[0]

            if field_info.get("primary_type") == "string" and "values" in field_info:
                field_info["cardinality"] = len(field_info["value"]) / len(sample_docs)

                # Remove actual values to save place
                del field_info["values"]
        # Check for indexes
        indexes = list(collection.list_indexes())
        index_info = [
            {
                "name": idx["name"],
                "keys": idx["key"],
                "unique": idx.get("unique", False),
                "sparse": idx.get("sparse", False)

            }
            for idx in indexes
        ]

        return {
            "fields": fields,
            "indexes": index_info,
            "document_count": collection.estimated_document_count(),
            "sample_size": len(sample_docs)
        }

    def _extract_field_info(self, doc: Dict[str, Any], fields: Dict[str, Any], prefix: str = "") -> None:
        self.logger.info("Analyze field infos....")
        stack = [(doc, prefix)]

        while stack:
            current_doc, current_prefix = stack.pop()
            for key, value in current_doc.items():
                if key == "_id" and not current_prefix:
                    continue

                field_name = f"{current_prefix}.{key}" if current_prefix else key
                field_info = fields.setdefault(field_name, {"types": Counter(), "null_count": 0, "min_value": None,
                                                            "max_value": None})

                if value is None:
                    field_info["null_count"] += 1
                    field_info["types"]["null"] += 1
                    continue

                value_type = type(value).__name__
                field_info["types"][value_type] += 1

                if isinstance(value, (int, float, datetime.datetime)):
                    field_info["min_value"] = min(filter(None, [field_info["min_value"], value]))
                    field_info["max_value"] = max(filter(None, [field_info["max_value"], value]))

                elif isinstance(value, str):
                    field_info.setdefault("values", set()).add(value)
                    if len(field_info["values"]) > 1000:
                        field_info["values"].pop()

                elif isinstance(value, dict):
                    stack.append((value, field_name))

                elif isinstance(value, list):
                    stats = field_info.setdefault("array_length", {"min": None, "max": None, "avg": 0, "count": 0})
                    length = len(value)
                    stats["count"] += 1
                    stats["avg"] = (stats["avg"] * (stats["count"] - 1) + length) / stats["count"]
                    stats["min"] = min(filter(None, [stats["min"], length]))
                    stats["max"] = max(filter(None, [stats["max"], length]))

                    field_info.setdefault("array_elements", {"types": Counter()})
                    for item in value[:5]:  # Analyze first 5 elements
                        if item is not None:
                            field_info["array_elements"]["types"][type(item).__name__] += 1
                            if isinstance(item, dict):
                                stack.append((item, f"{field_name}[].item"))

    def analyze_size(self, collection: Collection) -> Dict:
        """Analyze the size characteristics of the collection."""
        self.logger.info("Analyze the collection size")

        # Get collection stats
        stats = self.db.command("collStats", collection.name)
        sample_docs = list(collection.aggregate([{"$sample": {"size": 100}}]))
        avg_doc_size = 0
        if sample_docs:
            # Serialize documents to BSON to get accurate size
            avg_doc_size = sum(len(json_util.dumps(doc).encode('utf-8')) for doc in sample_docs) / len(sample_docs)

        return {
            "total_size_bytes": stats.get("size", 0),
            "storage_size_bytes": stats.get("storageSize", 0),
            "index_size_bytes": stats.get("totalIndexSize", 0),
            "avg_document_size_bytes": avg_doc_size,
            "document_count": stats.get("count", 0),
            "wired_tiger_stats": stats.get("wiredTiger", {})
        }


