import time
from datetime import datetime
import pandas as pd
import numpy as np
import duckdb
import argparse
import tracemalloc

# from queries.twitter_queries import
from prepare_database import prepare_database
import testing.tpch.setup as tpch_setup

# Paths and queries for different datasets
DATASETS = {
    "tpch": {
        "queries_map": tpch_setup.QUERIES,
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


def _results_dfs(dataset: str, tests: list[str]):
    old_results= dict()
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


def perform_tests(
        con: duckdb.DuckDBPyConnection,
        dataset: str,
        queries:list,
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

        results_df = pd.concat([results_df, temp_df],
                               ignore_index=True).reset_index(drop=True)
        print(f"""Query {i} Average Execution Time (last 4 runs): {
              avg_time:.4f} seconds""")
    return results_df, query_results


def compare_dataframes(dfs: list[pd.DataFrame]):
    """Compare two dataframes column by column."""

    # FIXME Should not be an issue, as we use postgres json extraction for all fields
    for cols in zip(*dfs):

        print(cols)
        for col in cols:
            print(col)
        print('----------------')
        pass


def compare_query_results(dfs: list[pd.DataFrame]):
    '''Compare the results of raw and materialized queries'''
    for i, (results) in enumerate(zip(*dfs), start=1):
        # Sort dataframes to ensure consistent ordering before comparison
        sorted_dfs = []
        try:
            for result_df in results:
                sorted_dfs.append(result_df.sort_values(
                    by=result_df.columns.tolist()).reset_index(drop=True))
        except Exception as e:
            # If sorting fails, proceed without sorting
            print("Sorting failed")
            sorted_dfs = results

        if compare_dataframes(dfs=sorted_dfs):
            print(f"Query {i}: Results match.")
        else:
            print(f"Query {i}: Results do not match.")
            for df in sorted_dfs:
                print(f"Query result:\n{df}")



def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run performance tests on different datasets.")
    parser.add_argument("dataset", nargs="?", default="tpch", choices=["tpch"],
                        help="The dataset to run tests on (tpch, yelp, twitter, or all)")
    args = parser.parse_args()

    # datasets_to_test = DATASETS.keys() if args.dataset == "all" else [
    #     args.dataset]
    datasets_to_test =['tpch']

    for dataset in datasets_to_test:

        config = DATASETS[dataset]

        queries_map: dict = config["queries_map"]
        tests_map: dict = config["tests_map"]
        column_map: dict = config["column_map"]

        db_connection = duckdb.connect(f"./data/db/{dataset}.db")

        # Load results dataframes and determine run number
        old_result_dfs, run_no = _results_dfs(dataset=dataset, tests=tests_map.keys())

        test_time = datetime.now()

        # Perform and log tests for raw data, collect results
        new_results_dfs = dict() # Test results
        query_results_dfs = dict() # Query results
        
        # Extract all fields to be used in these tests
        all_fields = list(set([field for query in queries_map.values() for field in query]))

        for test, test_config in tests_map.items():
            materialize_columns = test_config["materialization"]
            
            # Create the field-materialization setup for this test
            fields = []
            for field in all_fields:
                fields.append((field, column_map[field], field in materialize_columns))

            # Prepare database
            prepare_database(con=db_connection, fields=fields)

            # Run test
            new_results_df, query_result_df = perform_tests(
                con=db_connection, 
                dataset=dataset, 
                queries=queries_map.keys(), 
                run_no=run_no, 
                test_time=test_time
            )

            # Update df dicts
            old_result_dfs[test] = pd.concat([old_result_dfs[test], new_results_df], ignore_index=True)
            new_results_dfs[test] = new_results_df
            query_results_dfs[test] = query_result_df






        # Compare the results of raw and materialized queries
        print(f"\nComparing query results for dataset: {dataset}")
        compare_query_results(
            dfs=query_results_dfs.values()
        )
        print('-------')


if __name__ == "__main__":
    main()
