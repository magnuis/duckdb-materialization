import os
import random
import time
import shutil
from collections import defaultdict
from datetime import datetime


import pandas as pd
import duckdb

import testing.tpch.setup as tpch_setup
from prepare_database import prepare_database, get_db_size

TEST_TIME_STRING = f"{datetime.now().date()}-{datetime.now().hour}H"

DATASETS = {
    "tpch": {
        "queries": tpch_setup.QUERIES,
        "tests_map": tpch_setup.STANDARD_SETUPS,
        "column_map": tpch_setup.COLUMN_MAP,
        "db_backups": [
            {
                "scale_factor": 0.1,
                "dir": "tpch_small"
            },            {
                "scale_factor": 0.5,
                "dir": "tpch_medium"
            },
            {
                "scale_factor": 1,
                "dir": "tpch_big"
            }

        ]
    }
}

DF_COL_NAMES = [
    'Query',
    'Avg (last 4 runs)',
    'Iteration 0',
    'Iteration 1',
    'Iteration 2',
    'Iteration 3',
    'Iteration 4'
]

TESTS_PER_TRESHOLD = 3
TRESHOLDS_TO_MATERIALIZE = [0.25, 0.50, 0.75]

PATHS_TO_CLEAN = set()


def _prepare_dirs(dataset: str):
    if not os.path.exists(f"./results/verify-datasize-irrelevance"):
        os.mkdir(f"./results/verify-datasize-irrelevance")
    if not os.path.exists(f"./results/verify-datasize-irrelevance/{dataset}"):
        os.mkdir(f"./results/verify-datasize-irrelevance/{dataset}")
    if not os.path.exists(f"./results/verify-datasize-irrelevance/{dataset}/{TEST_TIME_STRING}"):
        os.mkdir(
            f"./results/verify-datasize-irrelevance/{dataset}/{TEST_TIME_STRING}")


def _generate_materializations(dataset: str, queries: list, columns: list):
    materializations = dict()

    # Generate test setup for the queries
    for i, query_name in enumerate(queries, start=1):
        query_setup = defaultdict(list)

        with open(f"./queries/{dataset}/{query_name}.sql", 'r') as f:
            query = f.read()

        columns_in_query = [col for col in columns if col in query]

        for treshold in TRESHOLDS_TO_MATERIALIZE:
            no_to_materialize = int(len(columns_in_query) * treshold)

            # Must materialize at least one
            no_to_materialize = max(no_to_materialize, 1)

            for seed in range(TESTS_PER_TRESHOLD):
                r = random.Random(x=seed)

                # FIXME Make sure there are not duplicates per treshold
                cols_to_materialize = r.sample(
                    columns_in_query, no_to_materialize)

                query_setup[treshold].append(cols_to_materialize)

        materializations[query_name] = query_setup

    return materializations


def _create_connection(db_dir: str) -> tuple[duckdb.DuckDBPyConnection, str]:
    original_db_path = f"./data/db/{db_dir}.duckdb"
    copy_db_path = f"./data/db/{db_dir}_test.db"

    PATHS_TO_CLEAN.add(copy_db_path)

    # Remove any old db
    if os.path.exists(copy_db_path):
        os.remove(copy_db_path)

    # Copy original db
    shutil.copy(original_db_path, copy_db_path)

    # Reconnect to db
    con = duckdb.connect(copy_db_path)
    con.execute("SET default_block_size = '16384'")

    con.execute("CHECKPOINT;")
    db_size = os.path.getsize(copy_db_path)
    # print(f"Fresh database size: {db_size/1024/1024:.6f} MB")

    return con, copy_db_path


def _perform_tests(db_dir: str, query: str, materializations: dict, con: duckdb.DuckDBPyConnection) -> tuple[dict, pd.DataFrame]:

    for strategies in materializations:

        db_connection, db_path = _create_connection(db_dir=db_dir)
    test_results = dict()
    iterations = 5
    query_result = None

    for i in range(iterations):
        start_time = time.perf_counter()
        result = con.execute(query).fetchdf
        end_time = time.perf_counter()
        execution_time = end_time - start_time

        test_results[f"Iteration {i}"] = execution_time

        if i == 0:

            query_result = result.copy()

    avg_time = sum(test_results[1:]) / (iterations - 1)
    test_results['Avg (last 4 runs)'] = avg_time

    print(f"""Query {i} Average Execution Time (last 4 runs): {
              avg_time:.4f} seconds""")

    return test_results, query_result


def _create_fresh_db(db_dir: str):
    db_path = f"./data/db/{db_dir}.duckdb"
    backup_path = f"./data/backup/{db_dir}"

    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed db at path {db_path}")

    with duckdb.connect(db_path) as con:
        con.execute("SET default_block_size = '16384'")
        con.execute(f"IMPORT DATABASE '{backup_path}';")

    PATHS_TO_CLEAN.add(db_path)


def main():

    dataset = "tpch"

    # Make sure directories exists
    _prepare_dirs(dataset=dataset)

    # Extract setup metadata
    config = DATASETS[dataset]
    queries: list = config["queries"]
    tests_map: dict = config["tests_map"]
    column_map: dict = config["column_map"]
    db_backups: dict = config["db_backups"]

    # Generate random materializations for each query
    materializations = _generate_materializations(
        dataset=dataset, queries=queries, columns=column_map.keys())

    for k, v in materializations.items():
        print(k)
        print(v)

    query_results_dfs = dict()  # Query results
    meta_results = []

    for db in db_backups:
        scale_factor = db["scale_factor"]
        dir = db["dir"]

        # Make a clean db to copy for each test
        _create_fresh_db(db_dir=dir)

        for query_name, strategies in materializations.items():
            with open(f"./queries/{dataset}/{query_name}.sql", 'r') as f:
                query = f.read()
            _perform_tests(query=query, materializations=strategies)


if __name__ == "__main__":
    main()
