import argparse
import time
import tracemalloc
import os
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd

import testing.tpch.setup as tpch_setup
# from queries.twitter_queries import
from prepare_database import prepare_database

if not os.path.isdir("./results"):
    os.mkdir("./results")
if not os.path.isdir("./results/tpch/old_duckdb"):
    os.mkdir("./results/tpch/old_duckdb")

# Paths and queries for different datasets
DATASETS = {
    "tpch": {
        "queries": tpch_setup.QUERIES,
        "tests_map": tpch_setup.TESTS,
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

    for i, query in enumerate(queries, start=1):

        # with open(f"./queries/{dataset}/{query_name}.sql", 'r') as f:
        #     query = f.read()

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
                print(f"Finished first test run in {execution_time} seconds")

        for j in range(iterations):
            df_row[f"malloc {j}"] = mallocs[j]

        # Collect the result from the first run
        query_results.append(first_run_result)

        # Calculate the average time of the last 4 runs and store it
        # avg_time = -1
        avg_time = sum(execution_times[1:]) / (iterations - 1)
        df_row['Avg (last 4 runs)'] = avg_time

        temp_df = pd.DataFrame([df_row])

        results_df = pd.concat([results_df, temp_df],
                               ignore_index=True).reset_index(drop=True)
        print(f"""Query {i} Average Execution Time (last 4 runs): {
              avg_time:.4f} seconds""")
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
            success = False

    return success


def perform_tests():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run performance tests on different datasets.")
    parser.add_argument("dataset", nargs="?", default="tpch", choices=["tpch"],
                        help="The dataset to run tests on (tpch, yelp, twitter, or all)")
    args = parser.parse_args()

    # datasets_to_test = DATASETS.keys() if args.dataset == "all" else [
    #     args.dataset]
    datasets_to_test = ['tpch']

    for dataset in datasets_to_test:
        if not os.path.isdir(f"./results/{dataset}"):
            os.mkdir(f"./results/{dataset}")



        db_connection = duckdb.connect(f"./data/db/{dataset}.db")

        test_time = datetime.now()

        # Perform and log tests for raw data, collect results
        new_results_dfs = dict()  # Test results
        query_results_dfs = dict()  # Query results

        # Extract all fields to be used in these tests
        # all_fields = list(set([field for query in queries_map.values() for field in query]))

        tests_map = {

            "view":{
                "queries": ["""
SELECT
    s.s_acctbal,
    s.s_name,
    n.n_name,
    p.p_partkey,
    p.p_mfgr,
    s.s_address,
    s.s_phone,
    s.s_comment
FROM
    test_view p,
    test_view s,
    test_view ps,
    test_view n,
    test_view r
WHERE
    p.p_partkey = ps.ps_partkey
    AND s.s_suppkey = ps.ps_suppkey
    AND p.p_size = 15
    AND p.p_type LIKE '%BRASS'
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = 'EUROPE'
    AND ps.ps_supplycost = (
        SELECT
            MIN(ps.ps_supplycost)
        FROM
            test_view s,
            test_view ps,
            test_view n,
            test_view r
        WHERE
            p.p_partkey = ps.ps_partkey
            AND s.s_suppkey = ps.ps_suppkey
            AND s.s_nationkey = n.n_nationkey
            AND n.n_regionkey = r.r_regionkey
            AND r.r_name = 'EUROPE'
    )
ORDER BY
    s.s_acctbal DESC,
    n.n_name,
    s.s_name,
    p.p_partkey
LIMIT
    100;

"""], "column_map":tpch_setup.COLUMN_MAP
            },
                       "raw":{
                "queries": ["""
            SELECT
            CAST(s.raw_json->>'s_acctbal' AS DECIMAL(12,2)) AS s_acctbal,
            s.raw_json->>'s_name' AS s_name,
            n.raw_json->>'n_name' AS n_name,
            CAST(p.raw_json->>'p_partkey' AS INT) AS p_partkey,
            p.raw_json->>'p_mfgr' AS p_mfgr,
            s.raw_json->>'s_address' AS s_address,
            s.raw_json->>'s_phone' AS s_phone,
            s.raw_json->>'s_comment' AS s_comment
     FROM
            test_table p,
            test_table s,
            test_table ps,
            test_table n,
            test_table r
     WHERE
            CAST(p.raw_json->>'p_partkey' AS INT) = CAST(ps.raw_json->>'ps_partkey' AS INT)
            AND CAST(s.raw_json->>'s_suppkey' AS INT) = CAST(ps.raw_json->>'ps_suppkey' AS INT)
            AND CAST(p.raw_json->>'p_size' AS INT) = 15
            AND (p.raw_json->>'p_type') like '%BRASS'
            AND CAST(s.raw_json->>'s_nationkey' AS INT) = CAST(n.raw_json->>'n_nationkey' AS INT)
            AND CAST(n.raw_json->>'n_regionkey' AS INT) = CAST(r.raw_json->>'r_regionkey' AS INT)
            AND (r.raw_json->>'r_name') = 'EUROPE'
            AND CAST(ps.raw_json->>'ps_supplycost' AS DECIMAL(12,2)) = (
                    SELECT
                            min(CAST(ps.raw_json->>'ps_supplycost' AS DECIMAL(12,2)))
                    FROM
                            test_table s,
                            test_table ps,
                            test_table n,
                            test_table r
                    WHERE
                            CAST(p.raw_json->>'p_partkey' AS INT) = CAST(ps.raw_json->>'ps_partkey' AS INT)
                            AND CAST(s.raw_json->>'s_suppkey' AS INT) = CAST(ps.raw_json->>'ps_suppkey' AS INT)
                            AND CAST(s.raw_json->>'s_nationkey' AS INT)= CAST(n.raw_json->>'n_nationkey' AS INT)
                            AND CAST(n.raw_json->>'n_regionkey' AS INT) = CAST(r.raw_json->>'r_regionkey' AS INT)
                            AND (r.raw_json->>'r_name') = 'EUROPE'

            )
    ORDER BY
            CAST(s.raw_json->>'s_acctbal' AS DECIMAL(12,2)) desc,
            n.raw_json->>'n_name',
            s.raw_json->>'s_name',
            CAST(p.raw_json->>'p_partkey' AS INT)
    LIMIT
            100;
"""],"column_map":{}
            },
        }

        for test, test_config in tests_map.items():

            # Create the field-materialization setup for this test
            fields = []
            for field, access_query in test_config["column_map"].items():
                fields.append(
                    (field, access_query, field in []))

            # Prepare database
            if len(fields) > 0:
                prepare_database(con=db_connection, fields=fields)

            # Run test
            new_results_df, query_result_df = _perform_test(
                con=db_connection,
                dataset=dataset,
                queries=test_config["queries"],
                run_no=1,
                test_time=test_time
            )

            print(test)
            print(dataset)

            new_results_df.to_csv(
                f"./results/{dataset}/old_duckdb/{test}.csv", index=False)

            # Update df dicts
            new_results_dfs[test] = new_results_df
            query_results_dfs[test] = query_result_df

        # Compare the results of raw and materialized queries
        print(f"\nComparing query results for dataset: {dataset}")
        success = compare_query_results(
            dfs=query_results_dfs.values()
        )
        print('-------')


if __name__ == "__main__":
    perform_tests()
