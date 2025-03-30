import logging
import json
import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


class PostgresTableSizeAnalyzer:
    def __init__(self, connection_params=None, table=None, schema='public', log_level=logging.INFO):
        """
        Initialize the PostgresTableSizeAnalyzer with connection parameters.
        Args:
            connection_params (dict): PostgreSQL connection parameters including:
                - host: Database host
                - port: Database port
                - database: Database name
                - user: Database user
                - password: Database password
            table (str): Table name
            schema (str): Schema name (default: 'public')
            log_level: Logging level
        """
        self.connection_params = connection_params
        self.table = table
        self.schema = schema
        # Setup logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("PostgresTableSizeAnalyzer")

    def analyze_table_size(self):
        """
        Analyze the table size:
        - total size in bytes
        - index size in bytes
        - row count
        - average row size
        - table bloat estimate
        Save the results in a json file with the given database and table name as part of the filename.
        """
        self.logger.info(
            f"Analyzing size of table {self.schema}.{self.table} in database {self.connection_params.get('database')}")

        # Connect to PostgreSQL
        try:
            conn = psycopg2.connect(**self.connection_params)
            cursor = conn.cursor()

            # Get table stats
            # Total table size (including indexes and toast)
            cursor.execute("""
                SELECT
                    pg_size_pretty(pg_total_relation_size(%s)) AS total_size,
                    pg_total_relation_size(%s) AS total_size_bytes,
                    pg_size_pretty(pg_relation_size(%s)) AS table_size,
                    pg_relation_size(%s) AS table_size_bytes,
                    pg_size_pretty(pg_indexes_size(%s)) AS index_size,
                    pg_indexes_size(%s) AS index_size_bytes,
                    (SELECT reltuples FROM pg_class WHERE oid = %s::regclass) AS estimated_row_count
                """,
                           [f"{self.schema}.{self.table}", f"{self.schema}.{self.table}",
                            f"{self.schema}.{self.table}", f"{self.schema}.{self.table}",
                            f"{self.schema}.{self.table}", f"{self.schema}.{self.table}",
                            f"{self.schema}.{self.table}"]
                           )
            stats = cursor.fetchone()

            # Get actual row count
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{self.table}")
            row_count = cursor.fetchone()[0]

            # Calculate average row size (if there are rows)
            avg_row_size = 0
            if row_count > 0:
                avg_row_size = stats[3] / row_count

            # Get additional table information
            cursor.execute("""
                SELECT 
                    t.relpages,
                    t.relallvisible,
                    t.reloptions,
                    pg_relation_filepath(t.oid) AS filepath,
                    s.n_live_tup,
                    s.n_dead_tup,
                    s.last_vacuum,
                    s.last_autovacuum,
                    s.last_analyze,
                    s.last_autoanalyze
                FROM pg_class t
                JOIN pg_stat_all_tables s ON t.oid = s.relid
                WHERE t.oid = %s::regclass
            """, [f"{self.schema}.{self.table}"])

            additional_info = cursor.fetchone()

            # Extract relevant information
            size_info = {
                "database": self.connection_params.get('database'),
                "schema": self.schema,
                "table": self.table,
                "timestamp": datetime.now().isoformat(),
                "row_count": row_count,
                "estimated_row_count": int(stats[6]),
                "total_size_pretty": stats[0],
                "total_size_bytes": int(stats[1]),  # Convert Decimal to int
                "table_size_pretty": stats[2],
                "table_size_bytes": int(stats[3]),  # Convert Decimal to int
                "index_size_pretty": stats[4],
                "index_size_bytes": int(stats[5]),  # Convert Decimal to int
                "avg_row_size_bytes": float(avg_row_size),  # Convert Decimal to float
                "pages": int(additional_info[0]) if additional_info[0] is not None else None,
                "all_visible_pages": int(additional_info[1]) if additional_info[1] is not None else None,
                "storage_parameters": additional_info[2],
                "file_path": additional_info[3],
                "live_tuples": int(additional_info[4]) if additional_info[4] is not None else None,
                "dead_tuples": int(additional_info[5]) if additional_info[5] is not None else None,
                "last_vacuum": additional_info[6].isoformat() if additional_info[6] else None,
                "last_autovacuum": additional_info[7].isoformat() if additional_info[7] else None,
                "last_analyze": additional_info[8].isoformat() if additional_info[8] else None,
                "last_autoanalyze": additional_info[9].isoformat() if additional_info[9] else None,
                "bloat_ratio": None  # Will be calculated below
            }

            # Simpler bloat estimate
            try:
                cursor.execute("""
                    SELECT 
                        round(((n_dead_tup::float / GREATEST(n_live_tup + n_dead_tup, 1)::float) * 100)::numeric, 2) AS bloat_estimate
                    FROM pg_stat_user_tables 
                    WHERE schemaname = %s AND relname = %s
                """, [self.schema, self.table])

                bloat_result = cursor.fetchone()
                if bloat_result and bloat_result[0] is not None:
                    size_info["dead_tuple_ratio"] = float(bloat_result[0])
            except Exception as e:
                self.logger.warning(f"Could not calculate dead tuple ratio: {str(e)}")

            # Save results to file
            filename = f"./output/{self.connection_params.get('database')}_{self.schema}_{self.table}_size_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w') as f:
                json.dump(size_info, f, indent=4, cls=DecimalEncoder)

            self.logger.info(f"Size analysis completed and saved to {filename}")
            return size_info

        except Exception as e:
            self.logger.error(f"Error analyzing table size: {str(e)}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
                self.logger.debug("PostgreSQL connection closed")

if __name__ == '__main__':
    connection_params = {
        'host': os.getenv('PG_HOST', 'localhost'),
        'port': os.getenv('PG_PORT', '5432'),
        'database': os.getenv('PG_DATABASE', 'postgres'),
        'user': os.getenv('PG_USER', 'postgres'),
        'password': os.getenv('PG_PASSWORD', 'root')
    }
    table = "playouts"
    schema = "playout_data"

    size_analyzer = PostgresTableSizeAnalyzer(connection_params=connection_params, table=table, schema=schema)
    results = size_analyzer.analyze_table_size()
    print(
        f"Table has {results['row_count']} rows with total size of {results['total_size_pretty']} "
        f"({results['table_size_pretty']} table + {results['index_size_pretty']} indexes)"
    )
