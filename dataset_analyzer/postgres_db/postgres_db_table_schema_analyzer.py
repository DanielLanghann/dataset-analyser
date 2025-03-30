import json
import logging
import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from typing import Dict, Any

class PostgresTableSchemaAnalyzer:
    """
    A class for analyzing the schema of a PostgreSQL table.
    """

    def __init__(self, connection_params=None, table=None, schema="public", log_level=logging.INFO):
        """
        Initialize the PostgreSQL schema analyzer.

        Args:
            connection_params: Database connection parameters
            table: Table name to analyze
            schema: Schema name containing the table
            log_level: Logging level
        """
        self.connection_params = connection_params
        self.table_name = table
        self.schema = schema

        # Configure logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("PostgresTableSchemaAnalyzer")

    def inspect_table_schema(self) -> Dict[str, Any]:
        """
        Retrieve and analyze the table schema.

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

    def save_schema_to_json(self, schema_info: Dict[str, Any], output_dir: str = "./output") -> str:
        """
        Save the schema information to a JSON file.

        Args:
            schema_info: Schema information to save
            output_dir: Directory to save the JSON file

        Returns:
            Path to the saved JSON file
        """
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{output_dir}/{self.connection_params.get('database')}_{self.schema}_{self.table_name}_schema_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(filename, 'w') as json_file:
            json.dump(schema_info, json_file, indent=4)

        self.logger.info(f"Schema analysis saved to {filename}")
        return filename

    def analyze_and_save_schema(self) -> str:
        """
        Analyze the table schema and save the result to a JSON file.

        Returns:
            Path to the saved JSON file
        """
        try:
            # Inspect table schema
            self.logger.info(f"Inspecting schema for {self.schema}.{self.table_name}")
            schema_info = self.inspect_table_schema()

            # Save schema to JSON file
            json_file_path = self.save_schema_to_json(schema_info)
            return json_file_path

        except Exception as e:
            self.logger.error(f"Error in schema analysis: {str(e)}")
            raise

def main():
    """
    Main function to run the PostgreSQL table schema analyzer.
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

    # Configure logging level
    log_level = os.getenv('PG_LOG_LEVEL', 'INFO')
    numeric_log_level = getattr(logging, log_level.upper(), logging.INFO)

    # Initialize and run analyzer
    schema_analyzer = PostgresTableSchemaAnalyzer(
        connection_params=connection_params,
        table=table,
        schema=schema,
        log_level=numeric_log_level
    )

    # Analyze schema and save to JSON
    json_file_path = schema_analyzer.analyze_and_save_schema()
    print(f"Schema analysis saved to {json_file_path}")

if __name__ == "__main__":
    main()
