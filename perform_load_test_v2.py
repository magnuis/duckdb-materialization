import ast
import copy
from datetime import datetime
import shutil
import time
import os
import random
from collections import defaultdict, OrderedDict
from collections.abc import Callable

import duckdb
import pandas as pd

from queries.query import Query
import testing.twitter.setup as twitter_setup
import testing.tpch.setup as tpch_setup
# from analyze_queries import analyze_queries
from prepare_database import prepare_database

if not os.path.isdir("./results"):
    os.mkdir("./results")

if not os.path.isdir("./results/load-based-N-fields"):
    os.mkdir("./results/load-based-N-fields")

QUERIES_IN_LOAD = 500
# MATERIALIZATION_SET_SIZES = [1, 2, 3, 4, 5, 6, 7, 8, 9,
#                              10, 11, 12, 13, 14, 15, 20, 25, 30, 35]
QUERY_PROPORTIONS = [4]
MAJORITY_PROPORTIONS = [0.80]
NO_LOADS = 10

READ_TIME = 0

BASE_PATH = os.curdir
PATHS_TO_REMOVE = []


USE_WEIGHTS_IN_DECISION = True
USE_PREV_TIME_IN_DECISION = True

TIME_WEIGHT = 36
TIME_CAP = 0.35


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


# def _numerical_distribution():

#     load_dicts = []

#     loads = []
#     majority_queries = []

#     for load_no in range(NO_LOADS):
#         load_path = BASE_PATH + f'/loads/without_q5/load{load_no}.txt'
#         with open(load_path, 'r') as file:
#             load_str = file.read()
#             load = ast.literal_eval(load_str)

#             loads.append(load)

#             majority_queries = _get_majority_queries(
#                 load=load, majority_size=4)

#     load_dicts.append({
#         "loads": loads,
#         "query_proportion": 4,
#         "majority_proportion": 400,
#         "majority_queries": majority_queries
#     })

#     return load_dicts


def _numerical_distribution(queries: dict[str, Query]):

    load_dicts = []

    all_queries = list(queries.keys())

    qm = [(q, int(m*QUERIES_IN_LOAD))
          for q in QUERY_PROPORTIONS for m in MAJORITY_PROPORTIONS]

    last_load_length = QUERIES_IN_LOAD

    for query_proportion, majority_proportion in qm:

        loads = []
        majority_queries = []

        for i in range(NO_LOADS):
            r = random.Random()
            r.seed(i)

            load_majority_queries = r.sample(
                population=all_queries, k=query_proportion, )
            minority_queries = [
                q for q in all_queries if q not in load_majority_queries]

            maj_load = [r.choice(load_majority_queries)
                        for _ in range(majority_proportion)]
            min_load = [r.choice(minority_queries)
                        for _ in range(QUERIES_IN_LOAD - majority_proportion)]

            load = maj_load + min_load

            r.shuffle(load)

            loads.append(load)
            majority_queries.append(
                sorted(load_majority_queries, key=lambda x: int(x[1:])))

            assert last_load_length == len(load)
            last_load_length = len(load)

        load_dicts.append({
            "loads": loads,
            "query_proportion": query_proportion,
            "majority_proportion": majority_proportion,
            "majority_queries": majority_queries
        })

    return load_dicts


def _random_distribution(queries: dict[str, Query]):

    load_dicts = []

    all_queries = list(queries.keys())

    qm = [(q, int(m*QUERIES_IN_LOAD))
          for q in QUERY_PROPORTIONS for m in MAJORITY_PROPORTIONS]

    last_load_length = QUERIES_IN_LOAD

    for query_proportion, majority_proportion in qm:

        loads = []
        majority_queries = []

        for i in range(NO_LOADS):
            r = random.Random()
            r.seed(i)

            load = [r.choice(all_queries) for _ in range(500)]

            r.shuffle(load)

            loads.append(load)
            majority_queries.append(None)

            assert last_load_length == len(load)
            last_load_length = len(load)

        load_dicts.append({
            "loads": loads,
            "query_proportion": query_proportion,
            "majority_proportion": majority_proportion,
            "majority_queries": majority_queries
        })

    return load_dicts


DISTRIBUTIONS: dict[str, Callable] = {
    "numerical": _numerical_distribution
    # "numerical": _random_distribution
}


def _generate_loads(distributions: list[int], queries: dict[str, Query]) -> dict[str, list[str]]:
    load_confs = defaultdict(list)
    for distribution in distributions:
        _load_method = DISTRIBUTIONS[distribution]
        load_dicts = _load_method(queries=queries)

        for load_dict in load_dicts:
            load_conf = {}
            load_conf["loads"] = load_dict["loads"]
            load_conf["query_proportion"] = load_dict["query_proportion"]
            load_conf["majority_proportion"] = load_dict["majority_proportion"]
            load_conf["majority_queries"] = load_dict["majority_queries"]
            load_confs[distribution].append(load_conf)

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


def _create_fresh_db(dataset: str):
    db_path = f"./data/db/{dataset}.duckdb"
    backup_path = f"./data/backup/{dataset}_medium"

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
    # FIXME REMOVE
    # con.execute(f"DELETE FROM test_table WHERE raw_json->>'id_str' != '';")
    # con.execute(
    #     f"DELETE FROM test_table WHERE raw_json->'delete'->>'timestamp_ms' != '';")

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

    dataset = "twitter"
    # dataset = "tpch"
    result_dir = BASE_PATH + \
        f"/results/load-based-v2/{dataset}/{TEST_TIME_STRING}"
    os.makedirs(result_dir, exist_ok=True)

    # Test time for all materializations of fields in all possible 0-3 tuples are previously ran.
    # Resuing these results for faster execution

    # prev_result_path = BASE_PATH + \
    #     f"/results/load-based-v2/{dataset}/2025-06-03-21H/results.csv"
    prev_result_path = BASE_PATH + \
        f"/results/load-based-v2/{dataset}/2025-06-04-1H/results.csv"

    try:
        prev_results_df = pd.read_csv(prev_result_path)
        pass
    except FileNotFoundError as e:
        assert False

        prev_results_df = pd.DataFrame(columns=["Query", "Last Materialization", "Load", "Test", "Materialization",
                                                "Iteration 0", "Iteration 1", "Iteration 2", "Iteration 3", "Iteration 4", "Average (last 4 runs)"])
    # Convert the Materilizations column to a list
    prev_results_df["Materialization"] = prev_results_df[["Materialization"]].apply(
        lambda row: set(ast.literal_eval(row["Materialization"])), axis=1)

    config = DATASETS[dataset]
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

    distributions = ["numerical"]  # TODO input-based

    load_types = _generate_loads(
        distributions=distributions, queries=queries)

    # Make sure query frequency exists
    # analyze_queries(data_set=dataset)

    # Get field distribution for each query
    # field_distribution = pd.read_csv(
    #     f'./results/{dataset}/field_distribution_{dataset}.csv')

    # Create fresh db
    _create_fresh_db(dataset=dataset)

    # field_distribution.set_index(keys=['query'])

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
                # field_weights = defaultdict(int)
                # for query_name, query_obj in queries.items():
                #     query_frequency = load.count(query_name)
                #     # TODO dynamic
                #     _field_weights = query_obj.get_column_weights(
                #         only_freq=not USE_WEIGHTS_IN_DECISION
                #     )
                #     for field, weight in _field_weights.items():
                #         field_weights[field] += weight * query_frequency

                #     # Sort fields by total weight
                # field_weights = dict(
                #     sorted(field_weights.items(), key=lambda item: item[1], reverse=True))
                # weighted_fields = list(field_weights.keys())

                # Update tests with materialization tests
                # for len_materialization in MATERIALIZATION_SET_SIZES:
                #     materialized_fields = weighted_fields[:len_materialization]

                #     tests[f"load_based_m{len_materialization}"] = {
                #         "materialization": materialized_fields,
                #         "last materialization": materialized_fields[-1]
                #     }
                for len_materialization in range(1, len(sorted_column_map.keys())):
                    # for len_materialization in MATERIALIZATION_SET_SIZES:
                    tests[f"load_based_m{len_materialization}"] = {
                        'len_materialization': len_materialization}
                    tests[f"schema_based_s{len_materialization}"] = {
                        'len_materialization': len_materialization}
                    tests[f"frequency_based_f{len_materialization}"] = {
                        'len_materialization': len_materialization}
                # Loop through the tests
                prev_materialization = set()
                for test_name, test_setup in tests.items():
                    load_test_execution_time = 0
                    if 'load_based_m' in test_name:
                        len_materialization = test_setup.get(
                            'len_materialization', -1)
                        assert len_materialization >= 0

                        # Take previous materialization time into consideration
                        no_fields_to_materialize = 1

                        # if len_materialization == 0:
                        #     prev_df = results_df[(
                        #         results_df["Load"] == load_no) & (results_df["Test"] == 'no_materialization')]
                        if len_materialization == 1:
                            prev_df = results_df[(
                                results_df["Load"] == load_no) & (results_df["Test"] == 'no_materialization')]
                        elif len_materialization >= 20:
                            no_fields_to_materialize = 5
                            prev_df = results_df[(
                                results_df["Load"] == load_no) & (results_df["Test"] == f'load_based_m{len_materialization-5}')]

                        else:
                            prev_df = results_df[(
                                results_df["Load"] == load_no) & (results_df["Test"] == f'load_based_m{len_materialization-1}')]

                        prev_materialization = prev_df['Materialization'].iloc[0]
                        prev_time = prev_df['Materialization'].iloc[0]

                        field_weights = defaultdict(int)
                        for query_name, query_obj in queries.items():
                            query_frequency = load.count(query_name)
                            # TODO dynamic
                            _field_weights = query_obj.get_column_weights(
                                only_freq=not USE_WEIGHTS_IN_DECISION,
                                prev_materialization=prev_materialization
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

                    elif 'frequency_based_f' in test_name:
                        len_materialization = test_setup.get(
                            'len_materialization', -1)
                        assert len_materialization >= 0

                        # Take previous materialization time into consideration
                        no_fields_to_materialize = 1

                        # if len_materialization == 0:
                        #     prev_df = results_df[(
                        #         results_df["Load"] == load_no) & (results_df["Test"] == 'no_materialization')]
                        if len_materialization == 1:
                            prev_df = results_df[(
                                results_df["Load"] == load_no) & (results_df["Test"] == 'no_materialization')]
                        elif len_materialization >= 20:
                            no_fields_to_materialize = 5
                            prev_df = results_df[(
                                results_df["Load"] == load_no) & (results_df["Test"] == f'load_based_m{len_materialization-5}')]

                        else:
                            prev_df = results_df[(
                                results_df["Load"] == load_no) & (results_df["Test"] == f'load_based_m{len_materialization-1}')]

                        prev_materialization = prev_df['Materialization'].iloc[0]
                        prev_time = prev_df['Materialization'].iloc[0]

                        field_weights = defaultdict(int)
                        for query_name, query_obj in queries.items():
                            query_frequency = load.count(query_name)
                            # TODO dynamic
                            _field_weights = query_obj.get_column_weights(
                                only_freq=True,
                                prev_materialization=prev_materialization
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
                    elif 'schema_based_s' in test_name:
                        len_materialization = test_setup.get(
                            'len_materialization', -1)
                        assert len_materialization >= 0

                        if len_materialization == 1:
                            prev_df = results_df[(
                                results_df["Load"] == load_no) & (results_df["Test"] == 'no_materialization')]
                        else:
                            prev_df = results_df[(
                                results_df["Load"] == load_no) & (results_df["Test"] == f'schema_based_s{len_materialization-1}')]

                        prev_materialization = prev_df['Materialization'].iloc[0]
                        weighted_load_test_fields = list(
                            sorted_column_map.keys())

                        fields_to_materialize = weighted_load_test_fields[:len_materialization]
                        last_materialization = fields_to_materialize[-1]

                    else:
                        fields_to_materialize = test_setup.get(
                            "materialization")
                    # fields_to_materialize_list = fields_to_materialize
                        last_materialization = test_setup.get(
                            "last materialization")

                    if fields_to_materialize is None:
                        fields_to_materialize = sorted_column_map.keys()
                    fields_to_materialize = set(fields_to_materialize)
                    # fields_to_materialize_set = set(fields_to_materialize)
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

                        # Check if test results exists
                        # Check results_df
                        filtered_result = results_df[(
                            results_df["Query"] == query_name) & (results_df["Materialization"].apply(lambda s: s == fields_to_materialize))]
                        # results_df["Query"] == query_name) & (results_df["Materialization"].apply(lambda s: s == fields_to_materialize_set))]

                        # Check prev_results_df
                        if len(filtered_result) <= 0:
                            # TODO REMOVE last if
                            # if 'load_based' in test_name or test_name in standard_tests:
                            filtered_result = prev_results_df[(
                                prev_results_df["Query"] == query_name) & (prev_results_df["Materialization"] == fields_to_materialize)]
                            # prev_results_df["Query"] == query_name) & (prev_results_df["Materialization"] == fields_to_materialize_set)]

                        # If we have a result, use it
                        if len(filtered_result) > 0:
                            result = filtered_result.iloc[0].copy()
                        else:
                            # If query was not affected by materialization, use result from prev materialization
                            if not query_affected:
                                result = results_df[(
                                    results_df["Query"] == query_name) & (results_df["Materialization"] == prev_materialization)].iloc[0]
                            # result = results_df[(
                            #     results_df["Query"] == query_name) & (results_df["Materialization"].apply(lambda s: s == prev_materialization))].iloc[0]
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

                loads_df.to_csv(result_dir + f"/load{load_no}_results.csv")

                print(
                    f"Time taken for load {load_no}: {time.time() - load_test_time}")

    # Write results to csv
    results_df.to_csv(result_dir + "/results.csv")


if __name__ == "__main__":
    t = time.perf_counter()

    main()

    print(
        f"Total time for load tests: {time.perf_counter() - t}")
