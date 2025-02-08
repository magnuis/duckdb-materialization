import json
import duckdb
import time
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Initialize file paths
JSON_FILE_PATH = './tpch/data/tpch_json.json'

DB_PATH = 'data/db/tpch.db'


PARQUET_FILE_PATH = './tpch/data/tpch_raw_json.parquet'


def create_db(con: duckdb.DuckDBPyConnection):
    # Drop the test table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS test_table")

    # Create the testtable table with one data colum
    con.execute("CREATE TABLE test_table (raw_json JSON)")
    print("Created fresh test_table table for raw JSON data.")


def parse_json_to_parquet(file_path, con: duckdb.DuckDBPyConnection, batch_size=50000) -> int:
    print("Starting to parse raw JSON file and prepare for Parquet conversion...")

    writer = None
    data_batch = []
    total_rows = 0

    try:
        # for file_path in JSON_FILE_PATHS:
        with open(file_path, 'r') as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    # Parse the JSON document and store as a single string
                    json_obj = json.loads(line)
                    data_batch.append({'raw_json': json.dumps(json_obj)})

                    # Once the batch reaches batch_size, write to Parquet
                    if len(data_batch) >= batch_size:
                        df_batch = pd.DataFrame(data_batch)
                        table_batch = pa.Table.from_pandas(df_batch)
                        with pq.ParquetWriter(PARQUET_FILE_PATH, table_batch.schema) as writer:
                            writer.write_table(table_batch)
                        insert_parquet_into_db(con=con, file_path=PARQUET_FILE_PATH)
                        total_rows += len(data_batch)
                        data_batch = []  # Clear batch memory
                        print(
                            f"Written {total_rows} rows to raw Parquet so far...")

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_number}: {e}")

            # Write any remaining rows in the last batch
            if data_batch:
                df_batch = pd.DataFrame(data_batch)
                table_batch = pa.Table.from_pandas(df_batch)
                with pq.ParquetWriter(PARQUET_FILE_PATH, table_batch.schema) as writer:
                    writer.write_table(table_batch)
                insert_parquet_into_db(con=con, file_path=PARQUET_FILE_PATH)
                total_rows += len(data_batch)
                print(f"""Final batch written. Total rows written to raw Parquet: {
                    total_rows}""")

    finally:
        # Close the writer if it was initialized
        if writer:
            writer.close()

    return total_rows


def insert_parquet_into_db(con: duckdb.DuckDBPyConnection, file_path=str) -> float:

    print("Starting to insert raw JSON Parquet data into DuckDB...")
    start_time = time.perf_counter()  # Start timing

    # Insert Parquet data into DuckDB
    con.execute("BEGIN TRANSACTION")
    con.execute(
        f"INSERT INTO test_table SELECT * FROM read_parquet('{file_path}')")
    con.execute("COMMIT")

    end_time = time.perf_counter()  # End timing
    total_time = end_time - start_time

    # print(f"Inserted {no_lines} rows of raw JSON data into DuckDB.")
    return total_time




def clean_up():
    os.remove(PARQUET_FILE_PATH)
    print(f"Deleted file {PARQUET_FILE_PATH}")




# Connect to or create the DuckDB instances
connection = duckdb.connect(DB_PATH)
# results_connection = duckdb.connect(RESULTS_DB_PATH)

# Clear and create required tables
create_db(con=connection)


# Parse the json into parquet
raw_lines = parse_json_to_parquet(con=connection, file_path=JSON_FILE_PATH)

# Delete created, unnecessary files
clean_up()
