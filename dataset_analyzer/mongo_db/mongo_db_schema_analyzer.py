import os
import json
import time
import sys
import argparse
from collections import defaultdict

from pymongo import MongoClient
import bson
from dotenv import load_dotenv


class SchemaAnalyzer:
    def __init__(self, uri=None, database=None, collection=None, sample_size=500):
        """
        Initialize the SchemaAnalyzer with connection parameters.

        Args:
            uri (str): MongoDB connection URI
            database (str): Database name
            collection (str): Collection name
            sample_size (int): Number of documents to sample for schema analysis
        """
        self.uri = uri
        self.database = database
        self.collection = collection
        self.sample_size = sample_size

        self.schema = defaultdict(lambda: defaultdict(int))
        self.field_examples = {}  # Store examples of each field's values
        self.doc_count = 0
        self.client = None
        self.nested_schema = None

    def get_bson_type(self, value):
        """Get a descriptive type for BSON/MongoDB specific data types"""
        if value is None:
            return "Null"
        elif isinstance(value, bson.objectid.ObjectId):
            return "ObjectId"
        elif isinstance(value, bson.datetime.datetime):
            return "Date"
        elif isinstance(value, list):
            if not value:  # Empty list
                return "Array(Empty)"
            # Check first item type
            if len(value) > 0:
                first_item = value[0]
                if isinstance(first_item, dict):
                    return "Array(Object)"
                else:
                    return f"Array({self.get_bson_type(first_item)})"
            return "Array"
        else:
            return type(value).__name__

    def connect(self):
        """Establish connection to MongoDB"""
        print(f"⏳ Connecting to MongoDB at {self.uri}...")
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self.client.server_info()  # Will raise exception if unable to connect
            print(f"✅ Connected to MongoDB successfully")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
            return False

    def fetch_sample_documents(self):
        """Fetch sample documents from the specified collection"""
        if not self.client:
            if not self.connect():
                return []

        db = self.client[self.database]
        collection = db[self.collection]

        # Get collection stats
        print(f"⏳ Getting collection statistics...")
        total_docs = collection.estimated_document_count()
        print(f"📊 Collection '{self.collection}' contains approximately {total_docs:,} documents")

        # Adjust sample size if needed
        actual_sample_size = min(self.sample_size, total_docs)
        print(f"🔍 Will analyze schema using {actual_sample_size:,} sample documents")

        # Retrieve sample documents with a timeout
        print(f"⏳ Retrieving sample documents from MongoDB...")
        sample_docs_cursor = collection.find(
            {},
            batch_size=20,
            limit=actual_sample_size
        ).max_time_ms(30000)  # 30 second timeout

        # Convert cursor to list - with progress indicator
        sample_documents = []
        print(f"⏳ Loading documents into memory...")
        for i, doc in enumerate(sample_docs_cursor):
            sample_documents.append(doc)
            if (i + 1) % 20 == 0 or i + 1 == actual_sample_size:
                progress = (i + 1) / actual_sample_size * 100
                sys.stdout.write(f"\r⏳ Loaded {i + 1}/{actual_sample_size} documents ({progress:.1f}%)")
                sys.stdout.flush()
        print()

        return sample_documents

    def analyze_documents(self, documents=None):
        """
        Analyze a list of documents and build schema.
        If documents is None, fetch samples from MongoDB.
        """
        if documents is None:
            documents = self.fetch_sample_documents()

        if not documents:
            print("❌ No documents to analyze")
            return None

        self.doc_count = len(documents)

        print(f"⏳ Starting schema analysis of {self.doc_count} documents...")
        start_time = time.time()

        # Process each document
        for i, doc in enumerate(documents):
            # Progress reporting
            if (i + 1) % 10 == 0 or i + 1 == self.doc_count:
                progress = (i + 1) / self.doc_count * 100
                elapsed = time.time() - start_time
                docs_per_second = (i + 1) / elapsed if elapsed > 0 else 0
                eta = (self.doc_count - (i + 1)) / docs_per_second if docs_per_second > 0 else "unknown"
                eta_str = f"{eta:.1f}s" if isinstance(eta, float) else eta

                sys.stdout.write(
                    f"\r⏳ Processed {i + 1}/{self.doc_count} documents ({progress:.1f}%) - {docs_per_second:.1f} docs/sec - ETA: {eta_str}")
                sys.stdout.flush()

            # Process the document completely (with full nesting)
            self.process_document(doc)

        print("\n✅ Completed document analysis")
        return self.finalize_schema()

    def process_document(self, doc, path_prefix=""):
        """Process a document and all its nested structures"""
        stack = [(path_prefix, doc, 0)]  # (path, value, depth)
        max_depth = 10  # Prevent infinite recursion

        while stack:
            current_path, current_value, depth = stack.pop()

            # Skip if we've gone too deep
            if depth > max_depth:
                continue

            if isinstance(current_value, dict):
                # Record this as a document/object
                if current_path:  # Skip the root document
                    self.schema[current_path]["Object"] += 1

                    # Store example if we don't have one yet (for object visualization)
                    if current_path not in self.field_examples:
                        # Store a simplified representation
                        self.field_examples[current_path] = {
                            k: self.simplify_for_example(v) for k, v in current_value.items()
                        }

                # Add all fields to the stack
                for key, value in current_value.items():
                    new_path = f"{current_path}.{key}" if current_path else key
                    stack.append((new_path, value, depth + 1))

            elif isinstance(current_value, list):
                # Record the array type
                if current_path:  # Skip the root level
                    array_type = self.get_bson_type(current_value)
                    self.schema[current_path][array_type] += 1

                    # Store example value
                    if current_path not in self.field_examples:
                        if len(current_value) > 0:
                            self.field_examples[current_path] = [
                                self.simplify_for_example(current_value[0])
                            ]
                        else:
                            self.field_examples[current_path] = []

                # Process array items (if they're objects)
                for idx, item in enumerate(current_value[:5]):  # Limit to first 5 items
                    if isinstance(item, dict):
                        # Use [] notation for array items
                        array_path = f"{current_path}[]"

                        # Process all fields in the array item
                        for key, value in item.items():
                            item_path = f"{array_path}.{key}"
                            stack.append((item_path, value, depth + 1))

            else:
                # Basic value type
                if current_path:  # Skip the root level
                    value_type = self.get_bson_type(current_value)
                    self.schema[current_path][value_type] += 1

                    # Store example value (if we don't have one yet)
                    if current_path not in self.field_examples:
                        self.field_examples[current_path] = self.simplify_for_example(current_value)

    def simplify_for_example(self, value):
        """Convert a value to a simplified representation for examples"""
        if isinstance(value, bson.objectid.ObjectId):
            return str(value)
        elif isinstance(value, bson.datetime.datetime):
            return value.isoformat()
        elif isinstance(value, dict):
            # Return a small subset of keys for dictionaries
            keys = list(value.keys())[:3]  # Only show up to 3 keys
            return {k: self.simplify_for_example(value[k]) for k in keys}
        elif isinstance(value, list):
            if not value:
                return []
            # Only show the first item for arrays
            return [self.simplify_for_example(value[0])]
        return value

    def finalize_schema(self):
        """Calculate statistics and finalize schema"""
        print("⏳ Finalizing schema statistics...")
        final_schema = {}

        for field_path, type_counts in self.schema.items():
            # Calculate type frequency percentages
            type_info = {}
            for type_name, count in type_counts.items():
                type_info[type_name] = {
                    "count": count,
                    "percentage": round((count / self.doc_count) * 100, 2)
                }

            # Sort types by frequency (descending)
            sorted_types = dict(sorted(
                type_info.items(),
                key=lambda item: item[1]["count"],
                reverse=True
            ))

            # Add field to final schema
            final_schema[field_path] = {
                "types": sorted_types,
                "coverage": round((sum(type_counts.values()) / self.doc_count) * 100, 2),
                "example": self.field_examples.get(field_path, None)
            }

        return final_schema

    def generate_nested_structure(self, schema=None):
        """Convert flat schema to nested structure for better visualization"""
        if schema is None:
            schema = self.finalize_schema()

        nested = {}

        # Sort fields to process parents before children
        for path in sorted(schema.keys()):
            field_info = schema[path]

            # Skip array item notation for the structure view
            if '[]' in path:
                continue

            parts = path.split('.')
            current = nested

            # Build the nested structure
            for i, part in enumerate(parts):
                if i == len(parts) - 1:
                    # This is the leaf field, add its type information
                    primary_type = next(iter(field_info["types"].keys()))
                    current[part] = {
                        "type": primary_type,
                        "coverage": field_info["coverage"]
                    }

                    # Add example if available
                    if "example" in field_info and field_info["example"] is not None:
                        current[part]["example"] = field_info["example"]
                else:
                    # This is a parent path, ensure it exists
                    if part not in current:
                        current[part] = {}
                    current = current[part]

        self.nested_schema = nested
        return nested

    def save_schema_to_files(self, flat_schema=None, nested_schema=None, output_dir="output"):
        """Save both flat and nested schema to files"""
        if flat_schema is None:
            flat_schema = self.finalize_schema()

        if nested_schema is None:
            nested_schema = self.generate_nested_structure(flat_schema)

        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save both flat and nested schema to files
        with open(f"{output_dir}/flat_schema.json", "w") as f:
            json.dump(flat_schema, f, indent=2, default=str)

        with open(f"{output_dir}/nested_schema.json", "w") as f:
            json.dump(nested_schema, f, indent=2, default=str)

        print("\n📋 Schema Analysis Complete!")
        print(f"Found {len(flat_schema)} distinct fields in the schema")
        print(f"✅ Saved flat schema to: {output_dir}/flat_schema.json")
        print(f"✅ Saved nested schema to: {output_dir}/nested_schema.json")

        return True

    def analyze_and_save(self):
        """Complete analysis workflow: connect, analyze, and save results"""
        start_time = time.time()

        try:
            # Connect to MongoDB
            if not self.connect():
                return False

            # Fetch and analyze documents
            flat_schema = self.analyze_documents()
            if not flat_schema:
                return False

            # Generate nested schema
            nested_schema = self.generate_nested_structure(flat_schema)

            # Save to files
            self.save_schema_to_files(flat_schema, nested_schema)

            # Print a preview of nested structure
            print("\n📌 Nested Structure Preview:")
            print(json.dumps(nested_schema, indent=2, default=str)[:1000] + "...\n")

            # Calculate and display execution time
            execution_time = time.time() - start_time
            print(f"\n⏱️  Total execution time: {execution_time:.2f} seconds")

            return True

        except KeyboardInterrupt:
            print("\n\n🛑 Operation cancelled by user")
            return False

        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            if self.client:
                self.client.close()
                print("🔒 MongoDB connection closed")

    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            print("🔒 MongoDB connection closed")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='MongoDB Schema Analyzer')
    parser.add_argument('--uri', help='MongoDB connection URI')
    parser.add_argument('--db', help='Database name')
    parser.add_argument('--collection', help='Collection name')
    parser.add_argument('--sample', type=int, default=500, help='Number of documents to sample (default: 500)')
    parser.add_argument('--output', default='output', help='Output directory (default: output)')
    return parser.parse_args()


def main():
    """Main entry point for command-line usage"""
    # Load environment variables
    load_dotenv()

    # Parse command line arguments
    args = parse_args()

    # Use command line arguments if provided, otherwise fall back to environment variables
    uri = args.uri or os.getenv("MONGODB_URI")
    database = args.db or os.getenv("MONGO_DB")
    collection = args.collection

    # Validate required parameters
    if not uri:
        print("❌ Error: MongoDB connection URI is required")
        print("   Provide it with --uri or set MONGODB_URI environment variable")
        return False

    if not database:
        print("❌ Error: Database name is required")
        print("   Provide it with --db or set MONGO_DB environment variable")
        return False

    if not collection:
        print("❌ Error: Collection name is required")
        print("   Provide it with --collection")
        return False

    print(f"📊 Using connection parameters:")
    print(f"   - Database: {database}")
    print(f"   - Collection: {collection}")
    print(f"   - Sample size: {args.sample}")

    # Create analyzer instance and run analysis
    analyzer = SchemaAnalyzer(
        uri=uri,
        database=database,
        collection=collection,
        sample_size=args.sample
    )

    result = analyzer.analyze_and_save()
    return result


if __name__ == "__main__":
    load_dotenv()
    analyzer = SchemaAnalyzer(uri=os.getenv('MONGODB_URI'), database=os.getenv('MONGO_DB'),
                              collection='device-playout')
    analyzer.analyze_and_save()
