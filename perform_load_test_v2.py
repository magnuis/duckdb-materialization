import ast
import copy
from datetime import datetime
import shutil
import time
import os
from collections import defaultdict
from collections.abc import Callable

import duckdb
import pandas as pd

from queries.query import Query, MaterializationStrategy
import testing.tpch.setup as tpch_setup
from analyze_queries import analyze_queries
from prepare_database import prepare_database, get_db_size

if not os.path.isdir("./results"):
    os.mkdir("./results")

if not os.path.isdir("./results/load-based-N-fields"):
    os.mkdir("./results/load-based-N-fields")

QUERIES_IN_LOAD = 500
MATERIALIZATION_SET_SIZES = [1, 2, 3, 4, 5, 6, 7, 8, 9,
                             10, 11, 12, 13, 14, 15, 20, 25, 30, 35]
QUERY_PROPORTIONS = [4]
MAJORITY_PROPORTIONS = [0.80]
NO_LOADS = 10

READ_TIME = 0

BASE_PATH = os.curdir
PATHS_TO_REMOVE = []


# Paths and queries for different datasets
DATASETS = {
    "tpch": {
        "queries": tpch_setup.QUERIES,
        "standard_tests": tpch_setup.STANDARD_SETUPS,
        "column_map": tpch_setup.COLUMN_MAP,
        "no_queries": len(tpch_setup.QUERIES)
    }
}

TEST_TIME_STRING = f"{datetime.now().date()}-{datetime.now().hour}H"


def _get_majority_queries(load: list[str], majority_size: int) -> list[str]:
    # Get a list of the unique queries in the workload
    queries = list(set(load))

    # Assert that `majority_size` does not exceed the number of unique queries
    assert majority_size < len(queries)

    # Count the occurence of each query in the workload
    queries_count = {query: load.count(query) for query in queries}

    # Sort queries_count
    sorted_queries_count = {query: count for query, count in sorted(
        queries_count.items(), key=lambda item: item[1], reverse=True)}

    return list(sorted_queries_count.keys())[:majority_size]


def _numerical_distribution():

    load_dicts = []

    loads = []
    majority_queries = []

    for load_no in range(NO_LOADS):
        load_path = BASE_PATH + f'/loads/without_q5/load{load_no}.txt'
        with open(load_path, 'r') as file:
            load_str = file.read()
            load = ast.literal_eval(load_str)

            loads.append(load)

            majority_queries = _get_majority_queries(
                load=load, majority_size=4)

    load_dicts.append({
        "loads": loads,
        "query_proportion": 4,
        "majority_proportion": 400,
        "majority_queries": majority_queries
    })

    return load_dicts


DISTRIBUTIONS: dict[str, Callable] = {
    "numerical": _numerical_distribution
}


def _generate_loads(distributions: list[int], queries: dict[str, Query]) -> dict[str, list[str]]:
    load_confs = defaultdict(list)
    for distribution in distributions:
        _load_method = DISTRIBUTIONS[distribution]
        load_dicts = _load_method()

        for load_dict in load_dicts:
            load_conf = {}
            load_conf["loads"] = load_dict["loads"]
            load_conf["query_proportion"] = load_dict["query_proportion"]
            load_conf["majority_proportion"] = load_dict["majority_proportion"]
            load_conf["majority_queries"] = load_dict["majority_queries"]
            load_confs[distribution].append(load_conf)

    return load_confs


def _calculate_field_frequency(load: list[str], field_distribution: pd.DataFrame):
    # Copy to not alter original df
    field_distribution = field_distribution.copy()
    field_names = field_distribution.columns.drop("query")

    # Calculate column frequency given the load
    query_frequency = defaultdict(int)
    # Calculate query frequency
    for query in load:
        query_frequency[query] += 1
    # Get column frequency
    for query, frequency in query_frequency.items():
        mask = field_distribution["query"] == query
        field_distribution.loc[mask, field_names] *= frequency

    # Get column frequency across all queries in load
    field_frequency = field_distribution.sum(axis=0, numeric_only=True)
    # Sort column frequencies
    field_frequency.sort_values(ascending=False, inplace=True)

    return field_frequency


def _create_fresh_db(dataset: str):
    db_path = f"./data/db/{dataset}.duckdb"
    backup_path = f"./data/backup/{dataset}_tiny"

    if os.path.exists(db_path):
        os.remove(db_path)

    with duckdb.connect(db_path) as con:
        con.execute(f"IMPORT DATABASE '{backup_path}';")


def _create_connection(dataset: str, test: str = "temp") -> tuple[duckdb.DuckDBPyConnection, str]:
    original_db_path = BASE_PATH + f"/data/db/{dataset}.duckdb"
    copy_db_path = BASE_PATH + f"/data/db/{dataset}_{test}.db"

    # Remove any old db
    if os.path.exists(copy_db_path):
        os.remove(copy_db_path)

    # Copy original db
    shutil.copy(original_db_path, copy_db_path)

    PATHS_TO_REMOVE.append(copy_db_path)

    # Reconnect to db
    con = duckdb.connect(copy_db_path)
    con.execute("SET default_block_size = '16384'")

    con.execute("CHECKPOINT;")

    return con, copy_db_path


def _test_execute_query(
    con: duckdb.DuckDBPyConnection,
    query: str,
    query_name: str,
    materialization: list[str]
):
    '''Execute the query 5 times and calculate average time of last 4 runs'''

    row = {
        "Query": query_name,
        "Materialization": materialization
    }

    execution_times = []

    iterations = 5

    for i in range(iterations):
        start_time = time.perf_counter()
        con.execute(query)
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        execution_times.append(execution_time)
        row[f"Iteration {i}"] = execution_time

    # Calculate the average time of the last 4 runs and store it
    # avg_time = -1
    avg_time = sum(execution_times[1:]) / (iterations - 1)
    row['Average (last 4 runs)'] = avg_time

    return row


def main():
    dataset = "tpch"
    result_dir = BASE_PATH + \
        f"/results/load-based-v2/{dataset}/{TEST_TIME_STRING}"
    os.makedirs(result_dir, exist_ok=True)

    # Test time for all materializations of fields in all possible 0-3 tuples are previously ran.
    # Resuing these results for faster execution
    option = int(input(
        "Do you want to use results for `json_extract_scalar` (1) or `->>` syntax (2)? "))
    if option == 1:
        # TODO Move results to a standard path
        prev_result_path = ''
        raise NotImplementedError()
    elif option == 2:
        # TODO Move results to a standard path
        prev_result_path = BASE_PATH + \
            "/results/single-queries/tpch/2025-05-10-15H/results.csv"
    else:
        raise ValueError("Option must be 1 or 2")

    prev_results_df = pd.read_csv(prev_result_path)
    # Convert the Materilizations column to a list
    prev_results_df["Materialization"] = prev_results_df[["Materialization"]].apply(
        lambda row: set(ast.literal_eval(row["Materialization"])), axis=1)

    config = DATASETS[dataset]
    standard_tests = config["standard_tests"]
    column_map = config["column_map"]
    queries: dict[str, Query] = config["queries"]

    distributions = ["numerical"]  # TODO input-based

    load_types = _generate_loads(
        distributions=distributions, queries=queries)

    # Make sure query frequency exists
    analyze_queries(data_set=dataset)

    # Get field distribution for each query
    field_distribution = pd.read_csv(
        f'./results/{dataset}/field_distribution_{dataset}.csv')

    # Create fresh db
    _create_fresh_db(dataset=dataset)

    field_distribution.set_index(keys=['query'])

    # results_df: pd.DataFrame = prev_results_df[prev_results_df["Materialization"] == set(
    # )]
    result_df_columns = [
        "Query", "Last Materialization", "Load", "Test", "Materialization", "Iteration 0", "Iteration 1", "Iteration 2", "Iteration 3", "Iteration 4", "Average (last 4 runs)"
    ]
    loads_df_columns = [
        "Load", "Test", "Total Query Time", "Majority Queries", "Materialization", "Strategy"
    ]

    results_df = pd.DataFrame(columns=result_df_columns)
    loads_df = pd.DataFrame(columns=loads_df_columns)

    db_connection, db_path = _create_connection(dataset=dataset)

    for distribution in distributions:

        # Get the simulated loads
        load_type = load_types[distribution]

        for load_setup in load_type:

            loads = load_setup["loads"]
            query_proportion = load_setup["query_proportion"]
            majority_proportion = load_setup["majority_proportion"]
            majority_queries = load_setup["majority_queries"]

            for load_no, load in enumerate(loads):
                load_test_time = time.time()

                # Copy the standard tests (zero materializations etc.)
                tests: dict = copy.deepcopy(standard_tests)

                # Generate the tests (materialization setups) fpr this load
                # FIXME revisit and rewrite this method
                # field_frequency = _calculate_field_frequency(
                #     load=load, field_distribution=field_distribution)

                # Decide materialization order of the fields
                # TODO make this dynamic, s.t. we can reuse for different strategies
                # field_frequency.sort_index(inplace=True)
                # field_frequency.sort_values(ascending=False, inplace=True)

                # Calculate weights
                field_weights = defaultdict(int)
                for query_name, query_obj in queries.items():
                    query_frequency = load.count(query_name)
                    # TODO dynamic
                    _field_weights = query_obj.get_column_weights(
                        strategy=MaterializationStrategy.FIRST_ITERATION)
                    for field, weight in _field_weights.items():
                        field_weights[field] += weight * query_frequency

                # Sort fields by total weight
                field_weights = dict(
                    sorted(field_weights.items(), key=lambda item: item[1], reverse=True))
                weighted_fields = list(field_weights.keys())

                # Update tests with materialization tests
                for no_fields_to_materialize in MATERIALIZATION_SET_SIZES:
                    materialized_fields = weighted_fields[:no_fields_to_materialize]

                    tests[f"load_based_m{no_fields_to_materialize}"] = {
                        "materialization": materialized_fields,
                        "last materialization": materialized_fields[-1]
                    }

                # Loop through the tests
                prev_materialization = set()
                for test_name, test_setup in tests.items():
                    load_test_execution_time = 0
                    fields_to_materialize = test_setup.get("materialization")
                    # fields_to_materialize_list = fields_to_materialize
                    last_materialization = test_setup.get(
                        "last materialization")

                    if fields_to_materialize is None:
                        fields_to_materialize = column_map.keys()
                    fields_to_materialize = set(fields_to_materialize)
                    # fields_to_materialize_set = set(fields_to_materialize)
                    # Create the field-materialization setup for this test
                    fields = []
                    for field, access_query in column_map.items():
                        fields.append(
                            (field, access_query, field in fields_to_materialize))

                    # Prepare database
                    prepare_database(con=db_connection, fields=fields)

                    # Loop through each query
                    for query_name, query_obj in queries.items():
                        result = None
                        query_affected = last_materialization in query_obj.columns_used(
                        ) or test_name in standard_tests

                        # Check if test results exists
                        # Check results_df
                        filtered_result = results_df[(
                            results_df["Query"] == query_name) & (results_df["Materialization"].apply(lambda s: s == fields_to_materialize))]
                        # results_df["Query"] == query_name) & (results_df["Materialization"].apply(lambda s: s == fields_to_materialize_set))]

                        # Check prev_results_df
                        if len(filtered_result) <= 0:
                            filtered_result = prev_results_df[(
                                prev_results_df["Query"] == query_name) & (prev_results_df["Materialization"] == fields_to_materialize)]
                            # prev_results_df["Query"] == query_name) & (prev_results_df["Materialization"] == fields_to_materialize_set)]

                        # If we have a result, use it
                        if len(filtered_result) > 0:
                            result = filtered_result.iloc[0]
                        else:
                            # If query was not affected by materialization, use result from prev materialization
                            if not query_affected:
                                result = results_df[(
                                    results_df["Query"] == query_name) & (results_df["Materialization"] == prev_materialization)].iloc[0]
                            # result = results_df[(
                            #     results_df["Query"] == query_name) & (results_df["Materialization"].apply(lambda s: s == prev_materialization))].iloc[0]
                            else:
                                # Perform test
                                query = query_obj.get_query(fields=fields)
                                result = _test_execute_query(
                                    con=db_connection,
                                    query=query,
                                    query_name=query_name,
                                    materialization=fields_to_materialize
                                )
                                print(
                                    f"Executed {query_name}, load {load_no} in time {result["Average (last 4 runs)"]}")

                        # Update results_df
                        if isinstance(result, dict):
                            result = pd.Series(
                                data=result, index=results_df.columns)

                        # Add load and test_name
                        result["Load"] = load_no
                        result["Test"] = test_name
                        result["Last Materialization"] = last_materialization

                        # Update with the fields materialized
                        result["Materialization"] = fields_to_materialize

                        # Convert result to a one-row df
                        result = pd.DataFrame(
                            data=[result], columns=result_df_columns)

                        results_df = pd.concat(
                            [results_df, result], ignore_index=True)

                        query_execution_time = result["Average (last 4 runs)"].iloc[0]
                        load_test_execution_time += query_execution_time * \
                            load.count(query_name)

                    # Update how long time the load "took"
                    _load_df = pd.DataFrame(data={
                        "Load": [load_no],
                        "Test": [test_name],
                        "Total Query Time": [query_execution_time],
                        "Majority Queries": [majority_queries],
                        "Materialization": [fields_to_materialize],
                        "Strategy": ["Frequency"]
                    }, columns=loads_df_columns)

                    loads_df = pd.concat(
                        [loads_df, _load_df], ignore_index=True)

                    # Set last materialization
                    if test_name not in standard_tests:
                        prev_materialization = fields_to_materialize

                loads_df.to_csv(result_dir + f"/load{load_no}_results.csv")

                print(
                    f"Time taken for load {load_no}: {load_test_time - time.time()}")

    # Write results to csv
    results_df.to_csv(result_dir + "/results.csv")


if __name__ == "__main__":
    t = time.perf_counter()

    main()

    print(
        f"Total time for load tests: {time.perf_counter() - t}")
