import argparse
import shutil
import time
import tracemalloc
import os
from datetime import datetime

import duckdb
import pandas as pd

import testing.tpch.setup as tpch_setup
import testing.yelp.setup as yelp_setup
# from queries.twitter_queries import
from prepare_database import prepare_database, get_db_size

if not os.path.isdir("./results"):
    os.mkdir("./results")

if not os.path.isdir("./results/single-queries"):
    os.mkdir("./results/single-queries")

TEST_TIME_STRING = f"{datetime.now().date()}-{datetime.now().hour}H"


# Paths and queries for different datasets
DATASETS = {
    "tpch": {
        "queries": tpch_setup.QUERIES,
        "tests_map": tpch_setup.STANDARD_SETUPS,
        "column_map": tpch_setup.COLUMN_MAP,
    },
    "yelp": {
        "queries": yelp_setup.QUERIES,
        "tests_map": yelp_setup.STANDARD_SETUPS,
        "column_map": yelp_setup.COLUMN_MAP,
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


def _results_dfs(dataset: str, tests: list[str]):
    old_results = dict()
    current_run_no = 1
    for test in tests:
        try:
            results = pd.read_csv(f"./testing/{dataset}/output/{test}.csv")
            run_no = results['Test run no.'].max() + 1

        except Exception:
            results = pd.DataFrame(columns=DF_COL_NAMES)
            run_no = 1

        current_run_no = max(current_run_no, run_no)
        old_results[test] = results

    return old_results, current_run_no


def _perform_test(
        con: duckdb.DuckDBPyConnection,
        dataset: str,
        queries: list,
        run_no: int,
        test_time: datetime,
) -> tuple[pd.DataFrame, list]:
    '''Perform the tests and collect results from the first execution'''
    # Execute each query 5 times and calculate average time of last 4 runs
    results_df = pd.DataFrame(columns=DF_COL_NAMES)
    query_results = []  # List to store results from the first execution

    for i, query_name in enumerate(queries, start=1):

        with open(f"./queries/{dataset}/{query_name}.sql", 'r') as f:
            query = f.read()

        df_row = {
            "Query": i,
            'Created At': test_time,
            'Test run no.': run_no,
        }

        execution_times = []
        first_run_result = None

        iterations = 5
        mallocs = []

        for j in range(iterations):  # Execute the query 5 times
            tracemalloc.start()
            start_time = time.perf_counter()
            # Execute the query and fetch result
            result = con.execute(query).fetchdf()
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            malloc = tracemalloc.get_traced_memory()[1]
            tracemalloc.stop()
            execution_times.append(execution_time)
            mallocs.append(malloc)
            df_row[f"Iteration {j}"] = execution_time

            if j == 0:
                first_run_result = result.copy()

        for j in range(iterations):
            df_row[f"malloc {j}"] = mallocs[j]

        # Collect the result from the first run
        query_results.append(first_run_result)

        # Calculate the average time of the last 4 runs and store it
        # avg_time = -1
        avg_time = sum(execution_times[1:]) / (iterations - 1)
        df_row['Avg (last 4 runs)'] = avg_time

        temp_df = pd.DataFrame([df_row])

        if results_df.empty:
            results_df = temp_df
        else:
            results_df = pd.concat([results_df, temp_df],
                                   ignore_index=True).reset_index(drop=True)
        # print(f"""Query {i} Average Execution Time (last 4 runs): {
        #       avg_time:.4f} seconds""")
    return results_df, query_results


def compare_dataframes(dfs: list[pd.DataFrame]):
    """Compare two dataframes column by column."""
    return all(df.equals(dfs[0]) for df in dfs)


def compare_query_results(dfs: list[pd.DataFrame]):
    '''Compare the results of raw and materialized queries'''

    success = True
    for i, (results) in enumerate(zip(*dfs), start=1):

        if compare_dataframes(dfs=results):
            print(f"Query {i}: Results match.")
        else:
            print(f"Query {i}: Results do not match.")
            for df in results:
                print(f"Query result:\n{df}")
                for col in df.columns:
                    print(f"Column {col}: {df[col].unique()}")
            success = False

    return success


def _create_fresh_db(dataset: str):
    db_path = f"./data/db/{dataset}.duckdb"
    backup_path = f"./data/backup/{dataset}_small"

    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed db at path {db_path}")

    with duckdb.connect(db_path) as con:
        con.execute("SET default_block_size = '16384'")
        con.execute(f"IMPORT DATABASE '{backup_path}';")


def _create_connection(dataset: str, test: str) -> tuple[duckdb.DuckDBPyConnection, str]:
    original_db_path = f"./data/db/{dataset}.duckdb"
    copy_db_path = f"./data/db/{dataset}_{test}.db"

    # Remove any old db
    if os.path.exists(copy_db_path):
        os.remove(copy_db_path)
        print(f"Removed db at path {copy_db_path}")

    # Copy original db
    shutil.copy(original_db_path, copy_db_path)

    # Reconnect to db
    con = duckdb.connect(copy_db_path)
    con.execute("SET default_block_size = '16384'")

    con.execute("CHECKPOINT;")
    db_size = os.path.getsize(copy_db_path)
    print(f"Fresh database size: {db_size/1024/1024:.6f} MB")

    return con, copy_db_path


def perform_tests():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run performance tests on different datasets.")
    parser.add_argument("dataset", nargs="?", default="tpch", choices=["tpch", "yelp"],
                        help="The dataset to run tests on (tpch, yelp, twitter, or all)")
    args = parser.parse_args()

    datasets_to_test = DATASETS.keys() if args.dataset == "all" else [
        args.dataset]
    # datasets_to_test = ['tpch']

    for dataset in datasets_to_test:
        if not os.path.exists(f"./results/single-queries/{dataset}"):
            os.mkdir(f"./results/single-queries/{dataset}")
        if not os.path.exists(f"./results/single-queries/{dataset}/{TEST_TIME_STRING}"):
            os.mkdir(f"./results/single-queries/{dataset}/{TEST_TIME_STRING}")

        config = DATASETS[dataset]

        queries: list = config["queries"]
        tests_map: dict = config["tests_map"]
        column_map: dict = config["column_map"]

        # Create fresh db
        _create_fresh_db(dataset=dataset)

        # db_connection.execute(
        #     f"ATTACH './data/db/{dataset}.db' AS original_db;")

        # Load results dataframes and determine run number
        old_result_dfs, run_no = _results_dfs(
            dataset=dataset, tests=tests_map.keys())

        test_time = datetime.now()

        # Perform and log tests for raw data, collect results
        new_results_dfs = dict()  # Test results
        query_results_dfs = dict()  # Query results

        # Extract all fields to be used in these tests
        # all_fields = list(set([field for query in queries_map.values() for field in query]))
        meta_results = []
        for test, test_config in tests_map.items():
            db_connection, db_path = _create_connection(
                dataset=dataset, test=test)
            materialize_columns = test_config["materialization"]
            if materialize_columns is None:
                materialize_columns = column_map.keys()

            # Create the field-materialization setup for this test
            fields = []
            for field, access_query in column_map.items():
                fields.append(
                    (field, access_query, field in materialize_columns))

            # Prepare database
            time_taken = prepare_database(
                con=db_connection, dataset=dataset, fields=fields)

            # Run test
            new_results_df, query_result_df = _perform_test(
                con=db_connection,
                dataset=dataset,
                queries=queries,
                run_no=run_no,
                test_time=test_time
            )

            new_results_df.to_csv(
                f"./results/single-queries/{dataset}/{TEST_TIME_STRING}/{test}.csv", index=False)

            # Update df dicts
            old_result_df: pd.DataFrame = old_result_dfs[test]
            if old_result_df.empty:
                old_result_dfs[test] = new_results_df
            else:
                old_result_dfs[test] = pd.concat(
                    [old_result_dfs[test], new_results_df], ignore_index=True)
            new_results_dfs[test] = new_results_df
            query_results_dfs[test] = query_result_df

            # Close db connection
            db_connection.execute("CHECKPOINT;")
            results = db_connection.execute(
                """
SELECT 
    'test_table' AS table_name,
    (SELECT block_size FROM pragma_database_size()) AS block_size,
    COUNT(DISTINCT block_id) AS num_blocks,
    COUNT(DISTINCT block_id) * (SELECT block_size FROM pragma_database_size()) AS num_bytes
FROM pragma_storage_info('test_table')
GROUP BY all
""").fetchdf()
            # print(results)
            # print("DB size after test:",
            #       db_connection.execute(
            #           "CALL pragma_database_size();").fetch_df())
            db_size = get_db_size(con=duckdb.connect(db_path))
            db_connection.close()

            db_size = os.path.getsize(db_path)

            print(f"Prepared database in time {time_taken:.2f}s")

            meta_results.append({
                "Test": test,
                "Time taken": time_taken,
                "Blocks used": db_size[0],
                "Block size": db_size[1],
                "Database size": db_size[2],
            })

            os.remove(db_path)

        meta_results_df = pd.DataFrame(meta_results)
        meta_results_df.to_csv(
            f"./results/single-queries/{dataset}/{TEST_TIME_STRING}/meta_results.csv", index=False)

        # Compare the results of raw and materialized queries
        print(f"\nComparing query results for dataset: {dataset}")
        success = compare_query_results(
            dfs=query_results_dfs.values()
        )


if __name__ == "__main__":
    perform_tests()
