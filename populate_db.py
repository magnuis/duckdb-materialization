import json
import duckdb
import time
import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CONFIG = {
    "tpch": {
        "json_path": './data/tpch/tpch_json_medium.json',
    }
}


DATA_PATH = './data'
DB_PATH = './data/db'
BACKUP_PATH = './data/backup'

CLEAN_UP_FILES = set()


def _create_db(con: duckdb.DuckDBPyConnection):
    # Drop the test table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS test_table")

    # Create the testtable table with one data colum
    con.execute("CREATE TABLE test_table (raw_json JSON)")
    print("Created fresh test_table table for raw JSON data.")


def _parse_and_insert(dataset: str, data_path: str, con: duckdb.DuckDBPyConnection, batch_size=50000) -> int:
    parquet_file_path = f"{DATA_PATH}/{dataset}.parquet"
    CLEAN_UP_FILES.add(parquet_file_path)

    def _insert_batch(data_batch: list[str]) -> int:
        df_batch = pd.DataFrame(data_batch)
        table_batch = pa.Table.from_pandas(df_batch)
        with pq.ParquetWriter(parquet_file_path, table_batch.schema) as writer:
            writer.write_table(table_batch)
        _insert_parquet_into_db(con=con, file_path=parquet_file_path)
        return len(data_batch)

    print("Starting to parse raw JSON file and prepare for Parquet conversion...")

    writer = None
    data_batch = []
    total_rows = 0

    try:
        # for file_path in JSON_FILE_PATHS:
        with open(data_path, 'r') as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    # Parse the JSON document and store as a single string
                    json_obj = json.loads(line)
                    data_batch.append({'raw_json': json.dumps(json_obj)})

                    # Once the batch reaches batch_size, write to Parquet
                    if len(data_batch) >= batch_size:
                        total_rows += _insert_batch(data_batch=data_batch)
                        data_batch = []  # Clear batch memory

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_number}: {e}")

            # Write any remaining rows in the last batch
            if data_batch:
                total_rows += _insert_batch(data_batch=data_batch)

    finally:
        # Close the writer if it was initialized
        if writer:
            writer.close()

    print(f"""Final batch written. Total rows written to raw Parquet: {
        total_rows}""")
    return total_rows


def _insert_parquet_into_db(con: duckdb.DuckDBPyConnection, file_path: str) -> float:

    # Insert Parquet data into DuckDB
    con.execute("BEGIN TRANSACTION;")

    # Insert data into db
    con.execute(
        f"INSERT INTO test_table SELECT raw_json FROM read_parquet('{file_path}');")
    con.execute("COMMIT;")


def _clean_up():
    print(CLEAN_UP_FILES)
    for file in list(CLEAN_UP_FILES):
        try:
            os.remove(file)
            print(f"Deleted file {file}")
        except FileNotFoundError:
            print(f"No file at path {file}")


def _prepare_dirs(dataset: str):
    if not os.path.isdir(DATA_PATH):
        os.mkdir(DATA_PATH)

    if not os.path.isdir(DB_PATH):
        os.mkdir(DB_PATH)

    backup_path = f"{BACKUP_PATH}/{dataset}_medium"
    try:
        os.mkdir(backup_path)
    except FileExistsError:
        shutil.rmtree(backup_path)
    except FileNotFoundError:
        os.mkdir(BACKUP_PATH)
        os.mkdir(backup_path)


def populate_db():
    """
    Populate database
    """
    dataset = 'tpch'  # TODO dynamic/take from args
    config = CONFIG[dataset]
    backup_path = f"{BACKUP_PATH}/{dataset}_medium"
    db_path = f"{DB_PATH}/{dataset}.db"

    # Prepare db and backup directories
    _prepare_dirs(dataset=dataset)

    db_connection = duckdb.connect(db_path)
    CLEAN_UP_FILES.add(db_path)

    _create_db(con=db_connection)

    _parse_and_insert(con=db_connection, dataset=dataset,
                      data_path=config["json_path"])

    # Export database as backup
    db_connection.execute(f"EXPORT DATABASE '{backup_path}' (FORMAT PARQUET);")
    # Close connection
    db_connection.close()

    _clean_up()


if __name__ == "__main__":
    populate_db()
