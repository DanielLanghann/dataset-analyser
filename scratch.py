import pymongo
import datetime
import numpy as np
from collections import Counter, defaultdict
import logging
from typing import Dict, List, Any, Optional, Tuple, Set
import json
from pymongo.database import Database
from pymongo.collection import Collection
from bson import json_util
import time
import concurrent.futures
from functools import lru_cache


class MongoDBAnalyzer:
    """Service to analyze MongoDB collections and extract key properties."""

    def __init__(self, mongodb_uri: str, db_name: str, log_level=logging.INFO):
        """
        Initialize the MongoDB analyzer service.

        Args:
            mongodb_uri: MongoDB connection URI
            db_name: Database name to analyze
            log_level: Logging level
        """
        self.client = pymongo.MongoClient(mongodb_uri)
        self.db = self.client[db_name]

        # Setup logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("MongoDBAnalyzer")

        # Store profiler status to restore later
        self.profiler_was_enabled = False

    def analyze_collection(self, collection_name: str, sample_size: int = 1000,
                           profiling_duration: int = 3600,
                           use_parallel: bool = True) -> Dict[str, Any]:
        """
        Analyze a MongoDB collection and extract key properties.

        Args:
            collection_name: Name of the collection to analyze
            sample_size: Number of documents to sample
            profiling_duration: Duration in seconds to collect profiling data
            use_parallel: Whether to use parallel processing for some analysis tasks

        Returns:
            Dictionary containing analysis results
        """
        self.logger.info(f"Starting analysis of collection: {collection_name}")
        collection = self.db[collection_name]

        results = {
            "collection_name": collection_name,
            "analysis_timestamp": datetime.datetime.now().isoformat()
        }

        # First run the access patterns analysis since it requires profiling
        # 3 & 4. Access Patterns and Query Complexity
        access_patterns, query_complexity = self._analyze_access_patterns(
            collection_name, profiling_duration
        )
        results["access_patterns"] = access_patterns
        results["query_complexity"] = query_complexity

        # Then run the other analyses in parallel if enabled
        if use_parallel and sample_size >= 500:  # Only use parallel for larger samples
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                # Submit analysis tasks
                structure_future = executor.submit(
                    self._analyze_data_structure, collection, sample_size
                )
                size_future = executor.submit(self._analyze_size, collection)
                consistency_future = executor.submit(self._analyze_consistency, collection)
                latency_future = executor.submit(self._analyze_latency, collection_name)
                relationships_future = executor.submit(self._analyze_relationships, collection)

                # Get results as they complete
                results["data_structure"] = structure_future.result()
                results["size"] = size_future.result()
                results["consistency_requirements"] = consistency_future.result()
                results["latency_requirements"] = latency_future.result()
                results["dataset_relationships"] = relationships_future.result()
        else:
            # Sequential processing
            results["data_structure"] = self._analyze_data_structure(collection, sample_size)
            results["size"] = self._analyze_size(collection)
            results["consistency_requirements"] = self._analyze_consistency(collection)
            results["latency_requirements"] = self._analyze_latency(collection_name)
            results["dataset_relationships"] = self._analyze_relationships(collection)

        self.logger.info(f"Analysis completed for collection: {collection_name}")
        return results

    def _analyze_data_structure(self, collection: Collection, sample_size: int) -> Dict:
        """Analyze the data structure of the collection."""
        self.logger.info("Analyzing data structure")

        # Adjust sample size based on collection size
        doc_count = collection.estimated_document_count()
        if doc_count < sample_size:
            sample_size = max(100, int(doc_count))
            self.logger.info(f"Adjusted sample size to {sample_size} based on collection size")

        # Get a sample of documents
        sample_docs = list(collection.aggregate([{"$sample": {"size": sample_size}}]))

        if not sample_docs:
            self.logger.warning("No documents found in collection")
            return {"fields": {}, "document_count": 0}

        # Extract schema information
        fields = {}

        # Process documents in chunks for better memory efficiency
        chunk_size = 100
        for i in range(0, len(sample_docs), chunk_size):
            chunk = sample_docs[i:i + chunk_size]
            for doc in chunk:
                self._extract_field_info(doc, fields)

        # Calculate field statistics
        sample_count = len(sample_docs)
        for field_name, field_info in fields.items():
            if "types" in field_info:
                # Calculate type distribution
                field_info["type_distribution"] = {
                    t: count / sample_count * 100
                    for t, count in field_info["types"].items()
                }

                # Determine primary type (most common)
                primary_type, _ = max(
                    field_info["types"].items(),
                    key=lambda x: x[1]
                )
                field_info["primary_type"] = primary_type

            # Calculate cardinality for string fields
            if field_info.get("primary_type") == "string" and "values" in field_info:
                field_info["cardinality"] = len(field_info["values"]) / sample_count
                # Only keep count of unique values, not the values themselves
                field_info["unique_values_count"] = len(field_info["values"])
                del field_info["values"]

        # Check for indexes and get their stats
        indexes = list(collection.list_indexes())
        index_info = []

        for idx in indexes:
            idx_info = {
                "name": idx["name"],
                "keys": idx["key"],
                "unique": idx.get("unique", False),
                "sparse": idx.get("sparse", False)
            }

            # Try to get index statistics if available
            try:
                idx_stats = collection.aggregate([
                    {"$indexStats": {"name": idx["name"]}}
                ]).next()
                idx_info["usage"] = {
                    "ops": idx_stats.get("accesses", {}).get("ops", 0),
                    "since": idx_stats.get("accesses", {}).get("since", None)
                }
            except Exception:
                # Index stats might not be available
                pass

            index_info.append(idx_info)

        # Identify potential primary key field
        primary_key = "_id"  # Default
        for field_name, field_info in fields.items():
            # Look for fields that have unique values in the sample
            if (field_info.get("unique_values_count", 0) == sample_count and
                    field_info.get("null_count", 0) == 0):
                for idx in indexes:
                    # Check if field has a unique index
                    if field_name in idx["key"] and idx.get("unique", False):
                        primary_key = field_name
                        break

        return {
            "fields": fields,
            "indexes": index_info,
            "document_count": doc_count,
            "sample_size": sample_count,
            "potential_primary_key": primary_key
        }

    def _extract_field_info(self, doc: Dict[str, Any], fields: Dict[str, Any], prefix: str = "") -> None:
        """
        Extract field information from document using an iterative approach.

        Args:
            doc: Document to analyze
            fields: Dictionary to populate with field information
            prefix: Field name prefix for nested fields
        """
        stack = [(doc, prefix)]

        while stack:
            current_doc, current_prefix = stack.pop()
            for key, value in current_doc.items():
                if key == "_id" and not current_prefix:
                    continue

                field_name = f"{current_prefix}.{key}" if current_prefix else key
                field_info = fields.setdefault(field_name, {
                    "types": Counter(),
                    "null_count": 0,
                    "min_value": None,
                    "max_value": None
                })

                if value is None:
                    field_info["null_count"] += 1
                    field_info["types"]["null"] += 1
                    continue

                value_type = type(value).__name__
                field_info["types"][value_type] += 1

                if isinstance(value, (int, float, datetime.datetime)):
                    # Update min/max for numeric and date fields
                    if field_info["min_value"] is None or value < field_info["min_value"]:
                        field_info["min_value"] = value
                    if field_info["max_value"] is None or value > field_info["max_value"]:
                        field_info["max_value"] = value

                elif isinstance(value, str):
                    # Track unique values for strings (limited to first 1000)
                    values_set = field_info.setdefault("values", set())
                    values_set.add(value)
                    if len(values_set) > 1000:  # Limit storage
                        values_set.pop()  # Remove an arbitrary element

                elif isinstance(value, dict):
                    # Add nested document to stack
                    stack.append((value, field_name))

                elif isinstance(value, list):
                    # Handle arrays
                    stats = field_info.setdefault("array_length", {"min": None, "max": None, "avg": 0, "count": 0})
                    length = len(value)
                    stats["count"] += 1
                    stats["avg"] = (stats["avg"] * (stats["count"] - 1) + length) / stats["count"]

                    if stats["min"] is None or length < stats["min"]:
                        stats["min"] = length
                    if stats["max"] is None or length > stats["max"]:
                        stats["max"] = length

                    # Analyze array elements (for first few items)
                    elements_info = field_info.setdefault("array_elements", {"types": Counter()})

                    for item in value[:5]:  # Limit to first 5 elements
                        if item is not None:
                            elements_info["types"][type(item).__name__] += 1

                            # If array contains objects, analyze their structure
                            if isinstance(item, dict):
                                stack.append((item, f"{field_name}[].item"))

    def _analyze_size(self, collection: Collection) -> Dict:
        """Analyze the size characteristics of the collection."""
        self.logger.info("Analyzing collection size")

        # Get collection stats
        stats = self.db.command("collStats", collection.name)

        # Sample documents to estimate average size
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


": len(profiler_data),
"avg_query_time_ms": np.mean(query_times) if query_times else 0,
"max_query_time_ms": max(query_times) if query_times else 0,
"query_time_percentiles": {
    "p50": np.percentile(query_times, 50) if query_times else 0,
    "p90": np.percentile(query_times, 90) if query_times else 0,
    "p99": np.percentile(query_times, 99) if query_times else 0
},
"complex_queries": sum(1 for entry in profiler_data
                       if "millis" in entry and entry["millis"] > 100)
}

# Prepare access patterns results
access_patterns = {
"operation_types": dict(operation_types),
"common_query_patterns": [
    {"fields": list(pattern), "count": count}
    for pattern, count in query_patterns.most_common(10)
],
"common_projection_patterns": [
    {"fields": list(pattern), "count": count}
    for pattern, count in projection_patterns.most_common(10)
],
"common_sort_patterns": [
    {"fields": list(pattern), "count": count}
    for pattern, count in sort_patterns.most_common(10)
],
"read_write_ratio": (
                            operation_types.get("query", 0) + operation_types.get("find", 0) +
                            operation_types.get("getmore", 0) + operation_types.get("count", 0)
                    ) / max(1, (
        operation_types.get("insert", 0) + operation_types.get("update", 0) +
        operation_types.get("remove", 0)
))
}

return access_patterns, query_complexity

finally:
# Restore previous profiler level
if self.profiler_was_enabled:
    self.db.command({"profile": 1, "slowms": 100})
else:
    self.db.command({"profile": 0})
self.logger.info("Profiler restored to previous state")


def _analyze_consistency(self, collection: Collection) -> Dict:
    """Analyze consistency requirements based on data and indexes."""
    self.logger.info("Analyzing consistency requirements")

    # Get information about unique indexes
    indexes = list(collection.list_indexes())
    unique_indexes = [idx for idx in indexes if idx.get("unique", False)]

    # Check for complex validation rules
    validation_rules = {}
    coll_info = self.db.command("listCollections", filter={"name": collection.name})
    if "cursor" in coll_info and "firstBatch" in coll_info["cursor"]:
        for info in coll_info["cursor"]["firstBatch"]:
            if "options" in info and "validator" in info["options"]:
                validation_rules = info["options"]["validator"]

    # Sample some documents to check for references
    sample_docs = list(collection.aggregate([{"$sample": {"size": 100}}]))
    ref_fields = set()

    # Heuristic to detect reference fields (ending with _id or containing 'ref')
    for doc in sample_docs:
        for field, value in self._flatten_dict(doc).items():
            if (field.endswith("_id") or "ref" in field.lower()) and isinstance(value, (str, int)):
                ref_fields.add(field)

    result = {
        "unique_constraints": [
            {
                "name": idx["name"],
                "fields": list(idx["key"].keys())
            }
            for idx in unique_indexes
        ],
        "validation_rules": validation_rules,
        "potential_reference_fields": list(ref_fields),
        "consistency_level": "unknown"  # Will be determined below
    }

    # Infer consistency level
    if unique_indexes or validation_rules:
        result["consistency_level"] = "high"
    elif ref_fields:
        result["consistency_level"] = "medium"
    else:
        result["consistency_level"] = "low"

    return result


def _flatten_dict(self, d: Dict, parent_key: str = '') -> Dict:
    """Flatten a nested dictionary."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(self._flatten_dict(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _analyze_latency(self, collection_name: str) -> Dict:
    """Analyze latency requirements based on query patterns."""
    self.logger.info("Analyzing latency requirements")

    # Get query execution times from profiler data
    profiler_data = list(self.db.system.profile.find({
        "ns": f"{self.db.name}.{collection_name}"
    }))

    query_times = [entry.get("millis", 0) for entry in profiler_data if "millis" in entry]

    if not query_times:
        return {
            "latency_requirements": "unknown",
            "reason": "No profiler data available"
        }

    # Calculate latency statistics
    latency_stats = {
        "avg_query_time_ms": np.mean(query_times),
        "p95_query_time_ms": np.percentile(query_times, 95) if query_times else 0,
        "p99_query_time_ms": np.percentile(query_times, 99) if query_times else 0,
        "max_query_time_ms": max(query_times) if query_times else 0
    }

    # Infer latency requirements
    latency_requirement = "medium"  # Default
    reason = "Based on average query patterns"

    if latency_stats["p99_query_time_ms"] < 10:
        latency_requirement = "high"
        reason = "99% of queries complete in less than 10ms, suggesting high latency requirements"
    elif latency_stats["p99_query_time_ms"] > 100:
        latency_requirement = "low"
        reason = "99% of queries take more than 100ms, suggesting relaxed latency requirements"

    return {
        "latency_statistics": latency_stats,
        "latency_requirements": latency_requirement,
        "reason": reason
    }


def _analyze_relationships(self, collection: Collection) -> Dict:
    """Analyze dataset relationships with other collections."""
    self.logger.info("Analyzing dataset relationships")

    # Sample documents to identify potential relationships
    sample_docs = list(collection.aggregate([{"$sample": {"size": 100}}]))
    if not sample_docs:
        return {"relationships": []}

    # Get all collection names in the database
    collections = self.db.list_collection_names()

    # Filter out system collections and the current collection
    collections = [c for c in collections if not c.startswith("system.") and c != collection.name]

    # Pre-compute singular forms and lowercase collection names for faster matching
    collection_forms = {}
    for coll_name in collections:
        collection_forms[coll_name] = {
            "name": coll_name,
            "singular": coll_name.rstrip('s'),
            "lower": coll_name.lower(),
            "singular_lower": coll_name.rstrip('s').lower()
        }

    # Look for potential references to other collections
    potential_refs = {}

    # First pass: identify potential references by field name
    for doc in sample_docs:
        flat_doc = self._flatten_dict(doc)
        for field, value in flat_doc.items():
            # Skip _id and non-reference looking fields
            field_lower = field.lower()
            if (field == "_id" or
                    (not field_lower.endswith("_id") and
                     "ref" not in field_lower and
                     "fk_" not in field_lower)):
                continue

            # Only process scalar values that could be references
            if not isinstance(value, (str, int)):
                continue

            # Check if field name matches any collection name patterns
            for coll_info in collection_forms.values():
                # Different matching patterns for field names
                if (coll_info["singular_lower"] in field_lower or
                        coll_info["lower"] in field_lower):

                    if field not in potential_refs:
                        potential_refs[field] = {
                            "referenced_collection": coll_info["name"],
                            "reference_type": "foreign_key",
                            "confidence": 0.5,  # Initial confidence
                            "sample_values": set(),  # Use set for faster lookups
                            "matches": 0,
                            "total_checked": 0
                        }

                    # Add sample value
                    potential_refs[field]["sample_values"].add(value)

                    # Increase confidence for exact matches
                    if field_lower == f"{coll_info['singular_lower']}_id":
                        potential_refs[field]["confidence"] = 0.9

    # Second pass: verify references by sampling (more efficient than checking all values)
    # Group by referenced collection for efficiency
    refs_by_collection = defaultdict(list)
    for field, ref_info in potential_refs.items():
        refs_by_collection[ref_info["referenced_collection"]].append((field, ref_info))

    # Check each referenced collection
    for coll_name, refs in refs_by_collection.items():
        ref_coll = self.db[coll_name]

        # Collect values to check for each field
        for field, ref_info in refs:
            # Get a sample of values to check (up to 5)
            sample_values = list(ref_info["sample_values"])[:5]
            ref_info["verified_samples"] = len(sample_values)

            # Skip if no sample values
            if not sample_values:
                continue

            # Check for matches
            for value in sample_values:
                try:
                    # Try to find the value in the _id field
                    if ref_coll.count_documents({"_id": value}, limit=1) > 0:
                        ref_info["matches"] += 1
                except Exception:
                    # Skip invalid queries
                    pass
                ref_info["total_checked"] += 1

            # Update confidence based on matches
            if ref_info["total_checked"] > 0:
                match_ratio = ref_info["matches"] / ref_info["total_checked"]
                # Scale confidence boost based on match ratio
                ref_info["confidence"] = min(1.0, ref_info["confidence"] + 0.5 * match_ratio)
                ref_info["verified_matches"] = ref_info["matches"]

    # Format results
    relationships = [
        {
            "field": field,
            "referenced_collection": info["referenced_collection"],
            "reference_type": info["reference_type"],
            "confidence": info["confidence"],
            "notes": (f"Found {info.get('verified_matches', 0)}/{info.get('verified_samples', 0)} "
                      f"matches in sample") if "verified_matches" in info else "Based on field naming"
        }
        for field, info in potential_refs.items()
    ]

    # Sort by confidence
    relationships.sort(key=lambda x: x["confidence"], reverse=True)

    return {
        "relationships": relationships,
        "relationship_complexity": "high" if len(relationships) > 5 else
        "medium" if len(relationships) > 1 else
        "low"
    }


2
f} | {rel['notes']} |\n
"
else:
md += "No relationships detected.\n"

return md

else:
# Default to JSON
return json.dumps(results, default=str, indent=2)


def analyze_database(self, excluded_collections=None) -> Dict[str, Any]:
    """
    Analyze all collections in the database.

    Args:
        excluded_collections: List of collection names to exclude

    Returns:
        Dictionary with analysis results for each collection
    """
    excluded = excluded_collections or ["system.profile", "system.views"]
    collections = [c for c in self.db.list_collection_names() if c not in excluded]

    self.logger.info(f"Analyzing {len(collections)} collections in database {self.db.name}")

    results = {}
    for collection_name in collections:
        try:
            self.logger.info(f"Starting analysis of collection: {collection_name}")
            results[collection_name] = self.analyze_collection(collection_name)
        except Exception as e:
            self.logger.error(f"Error analyzing collection {collection_name}: {str(e)}")
            results[collection_name] = {"error": str(e)}

    return results


def close(self):
    """Close MongoDB connection."""
    self.client.close()


# Example usage of the service
if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="MongoDB Collection Analyzer")
    parser.add_argument("--uri", help="MongoDB connection URI (can also use MONGO_URI env var)")
    parser.add_argument("--db", help="Database name to analyze (can also use MONGO_DB env var)")
    parser.add_argument("--collection", help="Collection name to analyze (optional)")
    parser.add_argument("--output", choices=["json", "markdown", "html"], default="json",
                        help="Output format (json, markdown, or html)")
    parser.add_argument("--profile-time", type=int, default=300,
                        help="Time in seconds to collect profiling data (default: 300)")
    parser.add_argument("--sample-size", type=int, default=1000,
                        help="Number of documents to sample for analysis (default: 1000)")
    parser.add_argument("--output-dir", default="./mongodb_analysis",
                        help="Directory to save output files (default: ./mongodb_analysis)")
    parser.add_argument("--parallel", action="store_true",
                        help="Use parallel processing for analysis tasks")
    parser.add_argument("--skip-profiling", action="store_true",
                        help="Skip the profiling step (faster but less comprehensive)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        default="INFO", help="Logging level")

    args = parser.parse_args()

    # Get MongoDB connection details from args or environment variables
    mongo_uri = args.uri or os.environ.get("MONGO_URI")
    mongo_db = args.db or os.environ.get("MONGO_DB")

    if not mongo_uri or not mongo_db:
        parser.error("MongoDB URI and database name are required. "
                     "Provide them using --uri and --db arguments "
                     "or set MONGO_URI and MONGO_DB environment variables.")

    # Setup output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Configure logging level
    log_level = getattr(logging, args.log_level)

    # Create analyzer
    analyzer = MongoDBAnalyzer(mongo_uri, mongo_db, log_level=log_level)

    # Set profiling duration
    profiling_duration = 0 if args.skip_profiling else args.profile_time

    try:
        if args.collection:
            # Analyze a single collection
            analyzer.logger.info(f"Analyzing collection: {args.collection}")

            # Generate and save report
            report = analyzer.generate_report(
                args.collection,
                args.output,
                sample_size=args.sample_size,
                profiling_duration=profiling_duration,
                use_parallel=args.parallel
            )

            # Save to file
            output_file = os.path.join(
                args.output_dir,
                f"{args.collection}_analysis.{args.output}"
            )
            with open(output_file, "w") as f:
                f.write(report)

            analyzer.logger.info(f"Analysis saved to {output_file}")

            # Also print to console if requested
            print(report)

        else:
            # Analyze all collections
            analyzer.logger.info(f"Analyzing all collections in database {mongo_db}")

            results = analyzer.analyze_database(
                sample_size=args.sample_size,
                profiling_duration=profiling_duration,
                use_parallel=args.parallel
            )

            # Save overall results as JSON
            json_output = os.path.join(args.output_dir, f"{mongo_db}_analysis.json")
            with open(json_output, "w") as f:
                json.dump(results, f, default=str, indent=2)

            analyzer.logger.info(f"Full analysis saved to {json_output}")

            # Generate individual reports for each collection
            if args.output != "json":
                for coll_name, coll_results in results.items():
                    if "error" not in coll_results:
                        report = analyzer._format_report(coll_results, args.output)
                        output_file = os.path.join(
                            args.output_dir,
                            f"{coll_name}_analysis.{args.output}"
                        )
                        with open(output_file, "w") as f:
                            f.write(report)

            # Print summary to console
            print(f"\nAnalysis of database '{mongo_db}' complete.")
            print(f"Results saved to {args.output_dir}/")
            print("\nCollection summaries:")

            for coll_name, coll_results in results.items():
                print(f"\n- {coll_name}")
                if "error" in coll_results:
                    print(f"  Error: {coll_results['error']}")
                else:
                    # Print a summary for each collection
                    try:
                        structure = coll_results["data_structure"]
                        size = coll_results["size"]

                        print(f"  Document count: {structure['document_count']:,}")
                        print(f"  Size: {size['total_size_bytes'] / (1024 * 1024):.2f} MB")
                        print(f"  Fields: {len(structure['fields'])}")
                        print(f"  Indexes: {len(structure['indexes'])}")

                        if "query_complexity" in coll_results:
                            complexity = coll_results["query_complexity"]
                            if "complexity_level" in complexity:
                                print(f"  Query complexity: {complexity['complexity_level']}")

                        if "dataset_relationships" in coll_results:
                            relationships = coll_results["dataset_relationships"]
                            rel_count = len(relationships.get("relationships", []))
                            print(f"  Relationships: {rel_count}")
                    except Exception as e:
                        print(f"  Error generating summary: {str(e)}")
    finally:
        analyzer.close()