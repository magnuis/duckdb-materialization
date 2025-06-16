# pylint: disable=E0401
import copy
from datetime import datetime
import shutil
import time
import os
from collections import defaultdict, OrderedDict
from collections.abc import Callable

import duckdb  # type: ignore
import pandas as pd

import testing.twitter.setup as twitter_setup
import testing.tpch.setup as tpch_setup
from queries.query import Query
import utils.generate_load as generate_load
from utils.prepare_database import prepare_database

if not os.path.isdir("./results"):
    os.mkdir("./results")

if not os.path.isdir("./results/load-based"):
    os.mkdir("./results/load-based")

MATERIALIZATION_SET_SIZES = [1, 2, 3, 4, 5, 6, 7, 8, 9,
                             10, 11, 12, 13, 14, 15, 20, 25, 30, 35]
WORKLOAD_DISTRIBUTION = 'numerical'
# WORKLOAD_DISTRIBUTION = 'random'

READ_TIME = 0
BASE_PATH = os.curdir
PATHS_TO_REMOVE = []
PHASE_3_ITERATION = 1


DISTRIBUTIONS: dict[str, Callable] = {
    "numerical": generate_load.numerical_distribution,
    "random": generate_load.random_distribution
}

USE_WEIGHTS_IN_DECISION = True
USE_PREV_TIME_IN_DECISION = True
DATASET = 'tpch'


# Paths and queries for different datasets
DATASETS = {
    "tpch": {
        "queries": tpch_setup.QUERIES,
        "standard_tests": tpch_setup.STANDARD_SETUPS,
        "column_map": tpch_setup.COLUMN_MAP,
        "no_queries": len(tpch_setup.QUERIES)
    },
    "twitter": {
        "queries": twitter_setup.QUERIES,
        "standard_tests": twitter_setup.STANDARD_SETUPS,
        "column_map": twitter_setup.COLUMN_MAP,
        "no_queries": len(twitter_setup.QUERIES)
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


def _generate_loads(distribution: str, queries: dict[str, Query]) -> dict[str, list[str]]:
    load_confs = []
    _load_method = DISTRIBUTIONS[distribution]
    load_dicts = _load_method(queries=queries)

    for load_dict in load_dicts:
        load_conf = {}
        load_conf["loads"] = load_dict["loads"]
        load_conf["query_proportion"] = load_dict["query_proportion"]
        load_conf["majority_proportion"] = load_dict["majority_proportion"]
        load_conf["majority_queries"] = load_dict["majority_queries"]
        load_confs.append(load_conf)

    return load_confs


def _calculate_field_priority(load: list[str], field_distribution: pd.DataFrame):
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


def _create_fresh_db():
    db_path = f"./data/db/{DATASET}.duckdb"
    backup_path = f"./data/backup/{DATASET}_tiny"

    if os.path.exists(db_path):
        os.remove(db_path)

    with duckdb.connect(db_path) as con:
        con.execute(f"IMPORT DATABASE '{backup_path}';")


def _create_connection(test: str = "temp") -> tuple[duckdb.DuckDBPyConnection, str]:
    original_db_path = BASE_PATH + f"/data/db/{DATASET}.duckdb"
    copy_db_path = BASE_PATH + f"/data/db/{DATASET}_{test}.db"

    # Remove any old db
    if os.path.exists(copy_db_path):
        os.remove(copy_db_path)

    # Copy original db
    shutil.copy(original_db_path, copy_db_path)

    PATHS_TO_REMOVE.append(copy_db_path)

    # Reconnect to db
    con = duckdb.connect(copy_db_path)

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
    if not USE_WEIGHTS_IN_DECISION:
        input("You are now only considering frequency in the tests. " +
              "If this is unintentional, please re-run with USE_WEIGHTS_IN_DECISION set to True")
    # if not USE_PREV_TIME_IN_DECISION:
    #     input("You are now only disregarding prev execution time in the tests. " +
    #           "If this is unintentional, please re-run with USE_PREV_TIME_IN_DECISION set to True")

    result_dir = BASE_PATH + \
        f"/results/load-based/{DATASET}/{TEST_TIME_STRING}"
    os.makedirs(result_dir, exist_ok=True)

    config = DATASETS[DATASET]
    standard_tests = config["standard_tests"]
    column_map = config["column_map"]
    queries: dict[str, Query] = config["queries"]

    # Sort column_map based on most frequently existing, then alphabetically
    sorted_items = sorted(
        column_map.items(),
        key=lambda kv: (kv[1]['frequency'], kv[0]),
        reverse=True
    )

    sorted_column_map = OrderedDict(sorted_items)

    # Create fresh db
    _create_fresh_db()

    # Create empty result columns
    result_df_columns = [
        "Query", "Last Materialization", "Load", "Test", "Materialization", "Iteration 0", "Iteration 1", "Iteration 2", "Iteration 3", "Iteration 4", "Average (last 4 runs)"
    ]
    loads_df_columns = [
        "Load", "Test", "Total Query Time", "Majority Queries", "Materialization", "Strategy"
    ]

    results_df = pd.DataFrame(columns=result_df_columns)
    loads_df = pd.DataFrame(columns=loads_df_columns)

    db_connection, db_path = _create_connection()

    # for distribution in distributions:

    load_setups = _generate_loads(
        distribution=WORKLOAD_DISTRIBUTION, queries=queries)

    for load_setup in load_setups:

        loads = load_setup["loads"]
        query_proportion = load_setup["query_proportion"]
        majority_proportion = load_setup["majority_proportion"]
        majority_queries = load_setup["majority_queries"]

        for load_no, load in enumerate(loads):
            load_test_time = time.time()

            # Copy the standard tests (zero materializations etc.)
            tests: dict = copy.deepcopy(standard_tests)

            m_sizes = []
            if DATASET == 'tpch':
                m_sizes = MATERIALIZATION_SET_SIZES
            elif DATASET == 'twitter':
                m_sizes = [i for i in range(1, len(sorted_column_map.keys()))]

            # Generate test setups
            for len_materialization in m_sizes:
                tests[f"load_based_m{len_materialization}"] = {
                    'len_materialization': len_materialization}
                if PHASE_3_ITERATION > 1:
                    tests[f"schema_based_s{len_materialization}"] = {
                        'len_materialization': len_materialization}
                    tests[f"frequency_based_f{len_materialization}"] = {
                        'len_materialization': len_materialization}

            # Loop through the tests
            prev_materialization = set()
            for test_name, test_setup in tests.items():
                load_test_execution_time = 0

                test_type = None
                if 'load_based_m' in test_name:
                    test_type = 'load_based_m'
                if 'frequency_based_f' in test_name:
                    test_type = 'frequency_based_f'
                if 'schema_based_s' in test_name:
                    test_type = 'schema_based_s'

                if test_type is not None:
                    len_materialization = test_setup.get(
                        'len_materialization', -1)
                    assert len_materialization >= 0

                    # Take previous materialization time into consideration
                    no_fields_to_materialize = 1

                    if len_materialization == 1:
                        prev_df = results_df[(
                            results_df["Load"] == load_no) & (results_df["Test"] == 'no_materialization')]
                    # Will only happen for TPC-H
                    elif len_materialization >= 20:
                        no_fields_to_materialize = 5
                        prev_df = results_df[(
                            results_df["Load"] == load_no) & (results_df["Test"] == f'{test_type}{len_materialization-5}')]

                    else:
                        prev_df = results_df[(
                            results_df["Load"] == load_no) & (results_df["Test"] == f'{test_type}{len_materialization-1}')]

                    prev_materialization = prev_df['Materialization'].iloc[0]
                    prev_time = prev_df['Materialization'].iloc[0]

                    field_weights = defaultdict(int)

                    if test_type == 'schema_based_s':
                        weighted_load_test_fields = list(
                            sorted_column_map.keys())

                    else:
                        only_freq = not USE_WEIGHTS_IN_DECISION
                        if test_type == 'frequency_based_f':
                            only_freq = True

                        for query_name, query_obj in queries.items():
                            query_frequency = load.count(query_name)
                            _field_weights = query_obj.get_column_weights(
                                only_freq=only_freq,
                                prev_materialization=prev_materialization,
                                iteration=PHASE_3_ITERATION
                            )
                            for field, weight in _field_weights.items():
                                field_weights[field] += weight * \
                                    query_frequency

                        # Remove prev materializations from field weights
                        load_test_field_weights = {key: val for key, val in field_weights.items(
                        ) if key not in prev_materialization}

                        # Sort fields by total weight
                        load_test_field_weights = dict(
                            sorted(load_test_field_weights.items(), key=lambda item: item[1], reverse=True))

                        weighted_load_test_fields = list(
                            load_test_field_weights.keys())

                    fields_to_materialize = list(prev_materialization) + \
                        weighted_load_test_fields[:no_fields_to_materialize]

                    last_materialization = fields_to_materialize[-1]

                else:
                    fields_to_materialize = test_setup.get(
                        "materialization")
                    last_materialization = test_setup.get(
                        "last materialization")

                if fields_to_materialize is None:
                    fields_to_materialize = sorted_column_map.keys()
                fields_to_materialize = set(fields_to_materialize)

                # Create the field-materialization setup for this test
                fields = []
                for field, access_query in sorted_column_map.items():
                    fields.append(
                        (field, access_query, field in fields_to_materialize))

                prepared_db = False

                # Loop through each query
                for query_name, query_obj in queries.items():
                    result = None
                    query_affected = last_materialization in query_obj.columns_used(
                    ) or test_name in standard_tests or len(fields_to_materialize) >= 15

                    # Check if test results exists already
                    filtered_result = results_df[(
                        results_df["Query"] == query_name) & (results_df["Materialization"].apply(lambda s: s == fields_to_materialize))]

                    # If we have a result, use it
                    if len(filtered_result) > 0:
                        result = filtered_result.iloc[0].copy()
                    else:
                        # If query was not affected by materialization, use result from prev materialization
                        if not query_affected:
                            result = results_df[(
                                results_df["Query"] == query_name) & (results_df["Materialization"] == prev_materialization)].iloc[0]
                        else:

                            # Prepare database, if not already done
                            if not prepared_db:
                                prepare_database(
                                    con=db_connection, fields=fields)
                                prepared_db = True
                            # Perform test
                            query = query_obj.get_query(fields=fields)
                            result = _test_execute_query(
                                con=db_connection,
                                query=query,
                                query_name=query_name,
                                materialization=fields_to_materialize
                            )
                            print(
                                f"Executed {query_name}, load {load_no}, test {test_name} in time {result['Average (last 4 runs)']}")

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
                    "Total Query Time": [load_test_execution_time],
                    "Majority Queries": [majority_queries[load_no]],
                    "Materialization": [fields_to_materialize],
                    "Strategy": ["Frequency"]
                }, columns=loads_df_columns)

                loads_df = pd.concat(
                    [loads_df, _load_df], ignore_index=True)

                # Set last materialization
                if test_name not in standard_tests:
                    prev_materialization = fields_to_materialize

                _load_df.to_csv(result_dir + f"/load_{load_no}_results.csv")

            print(
                f"Time taken for load {load_no}: {time.time() - load_test_time}")

    # Write results to csv
    results_df.to_csv(result_dir + "/results.csv")
    loads_df.to_csv(result_dir + "/all_loads_results.csv")


if __name__ == "__main__":
    t = time.perf_counter()

    main()

    print(
        f"Total time for load tests: {time.perf_counter() - t}")
