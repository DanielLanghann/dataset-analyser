import logging
import os
import time
import statistics
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional, Tuple


class PostgresTableLatencyAnalyzer:
    """
    A class for analyzing CRUD operation latency on PostgreSQL tables.
    Dynamically adapts to any table schema and generates appropriate test data.
    """

    def __init__(self, connection_params=None, table=None, schema="public", log_level=logging.INFO,
                 operations: int = 100, warmup_operations: int = 5):
        """
        Initialize the PostgreSQL latency analyzer.

        Args:
            connection_params: Database connection parameters
            table: Table name to analyze
            schema: Schema name containing the table
            log_level: Logging level
            operations: Number of operations to perform for measurements
            warmup_operations: Number of warmup operations before measurements
        """
        self.connection_params = connection_params
        self.table_name = table
        self.schema = schema
        self.operations = operations
        self.warmup_operations = warmup_operations

        # Configure logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("PostgresTableLatencyAnalyzer")

    def inspect_table_schema(self) -> Dict[str, Any]:
        """
        Retrieve and analyze the table schema to prepare appropriate test data.

        Returns:
            Dictionary containing table schema information
        """
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    # Query table columns and their data types with enhanced type information
                    cursor.execute(
                        """
                        SELECT 
                            c.column_name, 
                            c.data_type, 
                            c.is_nullable, 
                            c.column_default,
                            -- Get element type for arrays
                            CASE 
                                WHEN c.data_type = 'ARRAY' THEN
                                    (SELECT e.data_type FROM information_schema.element_types e
                                     WHERE e.object_catalog = c.table_catalog
                                       AND e.object_schema = c.table_schema
                                       AND e.object_name = c.table_name
                                       AND e.object_type = 'TABLE'
                                       AND e.collection_type_identifier = c.dtd_identifier)
                                ELSE NULL
                            END as array_element_type,
                            -- Get UDT name for custom types
                            c.udt_name,
                            -- Get character max length for varchar/char types
                            c.character_maximum_length
                        FROM information_schema.columns c
                        WHERE c.table_schema = %s AND c.table_name = %s
                        ORDER BY c.ordinal_position
                        """,
                        (self.schema, self.table_name)
                    )
                    columns = cursor.fetchall()

                    if not columns:
                        raise ValueError(f"Table {self.schema}.{self.table_name} not found or has no columns")

                    self.logger.info(f"Found {len(columns)} columns in {self.schema}.{self.table_name}")

                    # Identify primary key columns
                    cursor.execute(
                        """
                        SELECT a.attname
                        FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                        WHERE i.indrelid = %s::regclass AND i.indisprimary
                        """,
                        (f"{self.schema}.{self.table_name}",)
                    )
                    pk_columns = [row[0] for row in cursor.fetchall()]

                    schema_info = {
                        "columns": {
                            col[0]: {
                                "data_type": col[1],
                                "nullable": col[2] == 'YES',
                                "default": col[3],
                                "array_element_type": col[4],
                                "udt_name": col[5],
                                "char_max_length": col[6]
                            } for col in columns
                        },
                        "primary_key": pk_columns
                    }

                    # Debug log the schema info
                    for col_name, col_info in schema_info["columns"].items():
                        self.logger.debug(f"Column: {col_name}, Type: {col_info['data_type']}, "
                                          f"Array element type: {col_info['array_element_type']}, "
                                          f"UDT: {col_info['udt_name']}, "
                                          f"Max Length: {col_info['char_max_length']}")

                    return schema_info

        except Exception as e:
            self.logger.error(f"Error inspecting table schema: {str(e)}")
            raise

    def generate_test_data(self, schema_info: Dict[str, Any], iteration: int = 0) -> Dict[str, Any]:
        """
        Generate test data based on the table schema.

        Args:
            schema_info: Schema information from inspect_table_schema
            iteration: Iteration number to make test data unique

        Returns:
            Dictionary containing column name to test value mapping
        """
        test_data = {}

        for col_name, col_info in schema_info["columns"].items():
            # Skip columns with sequence defaults (usually auto-increment IDs)
            if col_info["default"] and "nextval" in str(col_info["default"]):
                continue

            # Generate appropriate data based on data type
            data_type = col_info["data_type"].lower()

            # For array types, we need special handling based on the element type
            if data_type == "array":
                element_type = col_info.get("array_element_type", "").lower()
                udt_name = col_info.get("udt_name", "")

                self.logger.debug(
                    f"Handling array column {col_name} with element type: {element_type}, UDT: {udt_name}")

                # For numeric arrays (numeric, int, float, etc.)
                if any(t in element_type or t in udt_name for t in ["numeric", "int", "float", "double", "decimal"]):
                    self.logger.debug(f"Generating numeric array for {col_name}")
                    test_data[col_name] = [10.5, 20.5, 30.5 + iteration]
                # For boolean arrays
                elif "bool" in element_type or "_bool" in udt_name:
                    test_data[col_name] = [True, False, True]
                # For text arrays
                elif any(t in element_type or t in udt_name for t in ["text", "char", "varchar"]):
                    test_data[col_name] = [f"item_{i}" for i in range(3)]
                # For other array types, use numeric values to be safe
                else:
                    self.logger.debug(
                        f"Using numeric array as default for {col_name} with type {element_type}/{udt_name}")
                    test_data[col_name] = [1.1, 2.2, 3.3 + iteration]

            elif "int" in data_type:
                test_data[col_name] = 42 + iteration
            elif any(t in data_type for t in ["text", "char", "varchar"]):
                # Check for character maximum length and respect it
                max_length = col_info.get("char_max_length")

                # Create a shorter test value that respects the max length
                if max_length:
                    # Keep the value short enough to fit within the column's maximum length
                    # We'll use a shorter prefix and add the iteration number
                    base_value = f"val_{col_name[:8]}_{iteration}"
                    # Ensure we don't exceed the maximum length
                    if len(base_value) > max_length:
                        # If we still exceed, create an even shorter value
                        base_value = f"v{iteration}_{col_name[:max_length - 5]}"
                        # Final safety check
                        if len(base_value) > max_length:
                            base_value = f"v{iteration}"[:max_length]

                    test_data[col_name] = base_value
                    self.logger.debug(
                        f"Generated varchar value for {col_name}: '{base_value}' (length: {len(base_value)}, max: {max_length})")
                else:
                    # For text types without length constraint
                    test_data[col_name] = f"test_value_{col_name}_{iteration}"
            elif "timestamp" in data_type or "date" in data_type:
                test_data[col_name] = datetime.now()
            elif "bool" in data_type:
                test_data[col_name] = True
            elif any(t in data_type for t in ["numeric", "float", "double", "decimal"]):
                test_data[col_name] = 42.42 + iteration
            elif "json" in data_type or "jsonb" in data_type:
                test_data[col_name] = {"test": f"value_{iteration}"}
            elif "uuid" in data_type:
                # Simple mock UUID for testing
                test_data[col_name] = f"00000000-0000-0000-0000-{iteration:012d}"
            # For other types, use a string representation
            else:
                # Create a reasonably short string for unknown types
                test_data[col_name] = f"test_{iteration}"

        return test_data

    def perform_crud_operations(self, schema_info: Dict[str, Any], warmup: bool = False) -> Dict[str, List[float]]:
        """
        Perform CRUD operations on the table and measure latency.

        Args:
            schema_info: Schema information from inspect_table_schema
            warmup: Whether these are warmup operations (not included in results)

        Returns:
            Dictionary with operation types and their latency measurements
        """
        results = {
            "insert": [],
            "select": [],
            "update": [],
            "delete": []
        }

        operations_count = self.warmup_operations if warmup else self.operations

        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    for i in range(operations_count):
                        # Generate test data for this iteration
                        test_data = self.generate_test_data(schema_info, i)

                        if not test_data:
                            self.logger.warning("Could not generate test data, skipping iteration")
                            continue

                        # Prepare column names and placeholders for INSERT
                        columns = list(test_data.keys())
                        values = list(test_data.values())
                        placeholders = ["%s"] * len(columns)

                        # Enable debug logging for test data
                        for col_name, value in zip(columns, values):
                            self.logger.debug(f"Column {col_name}: {value} (type: {type(value)})")

                        # Get primary key for later operations
                        pk_columns = schema_info["primary_key"]
                        if not pk_columns:
                            self.logger.warning("No primary key found, using all columns for WHERE clause")
                            pk_columns = list(test_data.keys())

                        # 1. INSERT
                        insert_sql = f'''
                            INSERT INTO "{self.schema}"."{self.table_name}" 
                            ({', '.join(f'"{col}"' for col in columns)}) 
                            VALUES ({', '.join(placeholders)}) 
                            RETURNING {', '.join(f'"{col}"' for col in pk_columns)};
                        '''

                        start_time = time.perf_counter()
                        cursor.execute(insert_sql, values)
                        pk_values = cursor.fetchone()
                        conn.commit()
                        end_time = time.perf_counter()

                        if not warmup:
                            results["insert"].append((end_time - start_time) * 1000)  # Convert to ms

                        # Create WHERE clause for primary key matching
                        where_clauses = []
                        where_values = []
                        for idx, pk_col in enumerate(pk_columns):
                            where_clauses.append(f'"{pk_col}" = %s')
                            where_values.append(pk_values[idx])

                        where_clause = " AND ".join(where_clauses)

                        # 2. SELECT
                        select_sql = f'''
                            SELECT * FROM "{self.schema}"."{self.table_name}" 
                            WHERE {where_clause};
                        '''

                        start_time = time.perf_counter()
                        cursor.execute(select_sql, where_values)
                        cursor.fetchall()  # Actually fetch the results
                        end_time = time.perf_counter()

                        if not warmup:
                            results["select"].append((end_time - start_time) * 1000)

                        # 3. UPDATE - Find a column to update
                        updateable_columns = []
                        for col in columns:
                            # Don't update primary key columns
                            if col not in pk_columns:
                                updateable_columns.append(col)

                        if updateable_columns:
                            # Pick the first non-PK column for update
                            update_col = updateable_columns[0]

                            # Generate appropriate update value based on column type
                            col_info = schema_info["columns"][update_col]
                            data_type = col_info["data_type"].lower()

                            if "text" in data_type or "char" in data_type or "varchar" in data_type:
                                # Check for character maximum length and respect it
                                max_length = col_info.get("char_max_length")
                                if max_length:
                                    # Create a short enough update value
                                    update_val = f"upd_{i}"
                                    if len(update_val) > max_length:
                                        update_val = f"u{i}"[:max_length]
                                else:
                                    update_val = f"updated_{update_col}_{i}"
                            elif "int" in data_type:
                                update_val = 1000 + i
                            elif "numeric" in data_type or "float" in data_type:
                                update_val = 99.99 + i
                            elif "bool" in data_type:
                                update_val = False
                            elif "array" in data_type:
                                element_type = col_info.get("array_element_type", "").lower()
                                udt_name = col_info.get("udt_name", "")

                                if any(t in element_type or t in udt_name for t in ["numeric", "int", "float"]):
                                    update_val = [100.1, 200.2, 300.3 + i]
                                elif "bool" in element_type:
                                    update_val = [False, True, False]
                                else:
                                    update_val = [f"updated_item_{j}" for j in range(3)]
                            else:
                                update_val = f"updated_{i}"

                            update_sql = f'''
                                UPDATE "{self.schema}"."{self.table_name}" 
                                SET "{update_col}" = %s
                                WHERE {where_clause};
                            '''

                            start_time = time.perf_counter()
                            cursor.execute(update_sql, [update_val] + where_values)
                            conn.commit()
                            end_time = time.perf_counter()

                            if not warmup:
                                results["update"].append((end_time - start_time) * 1000)
                        else:
                            self.logger.warning("No updateable columns found, skipping UPDATE test")

                        # 4. DELETE
                        delete_sql = f'''
                            DELETE FROM "{self.schema}"."{self.table_name}" 
                            WHERE {where_clause};
                        '''

                        start_time = time.perf_counter()
                        cursor.execute(delete_sql, where_values)
                        conn.commit()
                        end_time = time.perf_counter()

                        if not warmup:
                            results["delete"].append((end_time - start_time) * 1000)

                return results

        except Exception as e:
            self.logger.error(f"Error performing CRUD operations: {str(e)}")
            raise

    def calculate_statistics(self, results: Dict[str, List[float]]) -> Dict[str, Dict[str, Any]]:
        """
        Calculate statistics from the collected latency measurements.

        Args:
            results: Dictionary with operation types and their latency measurements

        Returns:
            Dictionary with statistics for each operation type
        """
        stats = {}

        for operation, times in results.items():
            if not times:
                self.logger.warning(f"No measurements for {operation}, skipping statistics")
                continue

            # Sort times for percentile calculations
            sorted_times = sorted(times)
            n = len(sorted_times)

            # Calculate percentiles safely
            p95_index = min(int(n * 0.95), n - 1)
            p99_index = min(int(n * 0.99), n - 1)

            stats[operation] = {
                "min": min(times),
                "max": max(times),
                "avg": statistics.mean(times),
                "median": statistics.median(times),
                "p95": sorted_times[p95_index],
                "p99": sorted_times[p99_index],
                "stddev": statistics.stdev(times) if len(times) > 1 else 0,
                "samples": len(times)
            }

        return stats

    def measure_postgres_latency(self) -> Dict[str, Dict[str, Any]]:
        """
        Main method to measure PostgreSQL latency.
        Performs table schema inspection, warmup operations, and actual measurements.

        Returns:
            Dictionary with statistics for each operation type
        """
        try:
            # 1. Load environment variables if not already loaded
            load_dotenv()

            # 2. Inspect table schema
            self.logger.info(f"Inspecting schema for {self.schema}.{self.table_name}")
            schema_info = self.inspect_table_schema()

            # 3. Perform warmup operations
            if self.warmup_operations > 0:
                self.logger.info(f"Performing {self.warmup_operations} warmup operations")
                self.perform_crud_operations(schema_info, warmup=True)

            # 4. Perform actual measurement operations
            self.logger.info(f"Performing {self.operations} measurement operations")
            results = self.perform_crud_operations(schema_info, warmup=False)

            # 5. Calculate and return statistics
            stats = self.calculate_statistics(results)

            return stats

        except Exception as e:
            self.logger.error(f"Error in latency measurement: {str(e)}")
            raise


def main():
    """
    Main function to run the PostgreSQL table latency analyzer.
    """
    # Load environment variables
    load_dotenv()

    # Get connection parameters from environment variables
    connection_params = {
        'host': os.getenv('PG_HOST', 'localhost'),
        'port': os.getenv('PG_PORT', '5432'),
        'database': os.getenv('PG_DATABASE', 'postgres'),
        'user': os.getenv('PG_USER', 'postgres'),
        'password': os.getenv('PG_PASSWORD', 'root')
    }

    # Table and schema to analyze
    table = os.getenv('PG_TABLE', 'playouts')
    schema = os.getenv('PG_SCHEMA', 'playout_data')

    # Configure operations
    operations = int(os.getenv('PG_OPERATIONS', '100'))
    warmup_operations = int(os.getenv('PG_WARMUP_OPERATIONS', '5'))

    # Configure logging level
    log_level = os.getenv('PG_LOG_LEVEL', 'INFO')
    numeric_log_level = getattr(logging, log_level.upper(), logging.INFO)

    # Initialize and run analyzer
    latency_analyzer = PostgresTableLatencyAnalyzer(
        connection_params=connection_params,
        table=table,
        schema=schema,
        operations=operations,
        warmup_operations=warmup_operations,
        log_level=numeric_log_level
    )

    # Measure latency
    stats = latency_analyzer.measure_postgres_latency()

    # Print results
    print("\nPostgreSQL Table Latency Results")
    print("=" * 50)
    print(f"Table: {schema}.{table}")
    print(f"Operations: {operations} (with {warmup_operations} warmup operations)")
    print("=" * 50)

    headers = ["Operation", "Min (ms)", "Max (ms)", "Avg (ms)", "Median (ms)", "P95 (ms)", "P99 (ms)", "StdDev"]
    print(
        f"{headers[0]:<10} {headers[1]:<10} {headers[2]:<10} {headers[3]:<10} {headers[4]:<10} {headers[5]:<10} {headers[6]:<10} {headers[7]:<10}")
    print("-" * 80)

    for operation, metrics in stats.items():
        print(f"{operation:<10} {metrics['min']:<10.2f} {metrics['max']:<10.2f} {metrics['avg']:<10.2f} "
              f"{metrics['median']:<10.2f} {metrics['p95']:<10.2f} {metrics['p99']:<10.2f} {metrics['stddev']:<10.2f}")

    return stats


if __name__ == "__main__":
    main()