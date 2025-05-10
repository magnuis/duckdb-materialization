import time
import os
from datetime import datetime
from itertools import combinations
from typing import Iterator, List

import duckdb
import pandas as pd

import testing.tpch.setup as tpch_setup
from queries.query import Query
from prepare_database import prepare_database

if not os.path.isdir("./results"):
    os.mkdir("./results")

if not os.path.isdir("./results/single-queries"):
    os.mkdir("./results/single-queries")

TEST_TIME_STRING = f"{datetime.now().date()}-{datetime.now().hour}H"
ITERATIONS = 5
PERMUTATION_SIZES = [0, 1, 2, 3]

DATASET = "tpch"


# Paths and queries for different datasets
DATASET_CONFIGS = {
    "tpch": {
        "queries": tpch_setup.QUERIES,
        "tests_map": tpch_setup.STANDARD_SETUPS,
        "column_map": tpch_setup.COLUMN_MAP,
    }
}

DF_COL_NAMES = [
    'Query',
    'Avg (last 4 runs)',
    'Iteration 0',
    'Iteration 1',
    'Iteration 2',
    'Iteration 3',
    'Iteration 4',
    'Created At',
    'Test run no.',
]


def _is_relevant_query(query: Query, materialization: list[str], q: str):
    query_columns = set(query.columns_used())
    # if q == 'q3':
    #     print(query_columns)
    #     for column_name in materialization:
    #         if not column_name in query_columns:
    #             print(f"Would have returned False for {column_name}")
    #         else:
    #             print(f"Would have continued for {column_name}")

    for column_name in materialization:
        if not column_name in query_columns:
            return False
    return True


def _perform_test(
        con: duckdb.DuckDBPyConnection,
        query: Query,
        query_name: str,
        materialization: list[str]
) -> pd.DataFrame:
    '''Execute the query 5 times and calculate average time of last 4 runs'''

    row = {
        "Query": query_name,
        "Materialization": materialization
    }

    execution_times = []

    # Execute the query 5 times
    for i in range(ITERATIONS):
        start_time = time.perf_counter()
        # Execute the query
        try:
            con.execute(query)
        except duckdb.duckdb.BinderException:
            print(query_name)
            print(query)
            assert False
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        execution_times.append(execution_time)
        row[f"Iteration {i}"] = execution_time

        # Calculate the average time of the last 4 runs and store it
        # avg_time = -1
        avg_time = sum(execution_times[1:]) / (ITERATIONS - 1)
        row['Average (last 4 runs)'] = avg_time

    return row


def _create_fresh_db():
    db_path = os.curdir + f"/data/db/{DATASET}_single_query_v2.duckdb"
    # TODO CHANGE
    test_db_path = os.curdir + f"/data/backup/{DATASET}_tiny"

    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed db at path {db_path}")

    con = duckdb.connect(db_path)
    con.execute(f"IMPORT DATABASE '{test_db_path}';")
    con.execute("CHECKPOINT;")

    return con, db_path


def _gen_combinations(strings: List[str]) -> Iterator[List[str]]:
    """
    Lazily yield every combination of 0, 1, 2, and 3 distinct elements
    from `strings` (order within each combination does not matter).
    """
    # max_k = min(len(strings), 3)
    for k in PERMUTATION_SIZES:
        if k == 0:
            yield []
        else:
            for combo in combinations(strings, k):
                yield list(combo)


def perform_test():

    result_dir_path = os.curdir + \
        f"/results/single-queries/{DATASET}/{TEST_TIME_STRING}"
    result_path = result_dir_path + '/results.csv'
    if not os.path.exists(result_dir_path):
        os.mkdir(result_dir_path)

    config = DATASET_CONFIGS[DATASET]

    queries: dict[str, Query] = config["queries"]
    column_map: dict = config["column_map"]
    column_list = list(column_map.keys())

    # Create fresh db
    db_connection, db_path = _create_fresh_db()

    m_no = -1
    # Loop through all combinations of possible materializations
    for materialize_columns in _gen_combinations(column_list):
        relevant_queries = 0
        test_time = time.time()
        prepared_db = False
        result_rows = []

        # Create the field-materialization setup for this test
        fields = []
        # print(
        #     f"Generating fields, starting from {fields}. Materialize columns is {materialize_columns}")
        for field, access_query in column_map.items():
            fields.append(
                (field, access_query, field in materialize_columns))

        # Iterate over the queries
        for query_name, query_obj in queries.items():
            query = query_obj.get_query(fields=fields)
            # print(query)

            # Check if the materialization is relevant for the current query
            if _is_relevant_query(query=query_obj, materialization=materialize_columns, q=query_name):
                relevant_queries += 1
                # Only prepare db if it is not prepared, and there is a relevant query
                if not prepared_db:
                    # print(
                    #     f"Preparing database for materialization {materialize_columns}")
                    prepare_database(con=db_connection, fields=fields)
                    prepared_db = True
            # print(f"{query_name}, materialize_columns:{materialize_columns}")
            # Run test
                result = _perform_test(
                    con=db_connection,
                    query=query,
                    query_name=query_name,
                    materialization=materialize_columns,
                )
                result_rows.append(result)

        # Append results to csv, if any tests ran
        if len(result_rows) > 0:
            # Create DataFrame
            temp_df = pd.DataFrame(result_rows)
            # Append to csv
            temp_df.to_csv(
                result_path,
                mode='a',
                header=not os.path.exists(result_path),
                index=False
            )
            m_no += 1
            print(
                f"Time taken for m{m_no}: {round(time.time()-test_time, 2)}s ({relevant_queries} queries)")

    # Close db connection and clean up
    db_connection.close()
    os.remove(db_path)


if __name__ == "__main__":
    t = time.time()
    perform_test()
    print(f"Total time taken for test: {round(time.time() - t)} seconds")
