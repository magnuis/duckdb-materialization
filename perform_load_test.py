import random
import copy
import argparse
from datetime import datetime
import shutil
import time
import os
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime

import duckdb
import pandas as pd

import testing.tpch.setup as tpch_setup
from analyze_queries import analyze_queries
from prepare_database import prepare_database

if not os.path.isdir("./results"):
    os.mkdir("./results")

MATERIALIZE_TRESHOLDS = [0.33, 0.5, 0.67, 0.8]
NO_QUERIES = 100

READ_TIME = 0

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


# DATASETS = {
#     "tpch": {
#         "no_queries": 25
#     }
# }


def _random_distribution(no_queries: int):
    return []


def _normalized_distribution(no_queries: int):
    return []


def _numerical_distribution(no_queries: int):

    load_dicts = []

    query_proportions = [3, 4, 5]
    majority_proportions = [80, 90]

    # try:
    #     query_proportion = int(input(
    #         f"How many of the {no_queries} should the majority of the load come from? "))
    #     if not 0 <= query_proportion <= no_queries:
    #         raise TypeError
    # except TypeError:
    #     raise ValueError(
    #         f"Query percentage must be an int between 0 and {no_queries}")

    # try:
    #     majority_proportion = int(
    #         input("What should the load proportion of the majority be? "))
    #     if not 50 <= majority_proportion <= 100:
    #         raise TypeError

    #     majority_proportion = int((majority_proportion/100) * NO_QUERIES)
    # except TypeError:
    #     raise ValueError("Query percentage must be an int between 50 and 100")

    all_queries = [f"q{i}" for i in range(1, no_queries + 1)]

    qm = [(q, m) for q in query_proportions for m in majority_proportions]

    for query_proportion, majority_proportion in qm:

        loads = []

        for i in range(5):
            r = random.Random()
            r.seed(i)

            majority_queries = r.sample(
                population=all_queries, k=query_proportion, )
            minority_queries = [
                q for q in all_queries if q not in majority_queries]

            load = [r.choice(majority_queries)
                    for _ in range(majority_proportion)]
            load += [r.choice(minority_queries)
                     for _ in range(NO_QUERIES - majority_proportion)]

            r.shuffle(load)

            loads.append(load)

        load_dicts.append({
            "loads": loads,
            "query_proportion": query_proportion,
            "majority_proportion": majority_proportion
        })

    return load_dicts


DISTRIBUTIONS: dict[str, Callable] = {
    "random": _random_distribution,
    "normalized": _normalized_distribution,
    "numerical": _numerical_distribution
}


def _generate_loads(distributions: list[int], no_queries: int) -> dict[str, list[str]]:
    load_confs = defaultdict(list)
    for distribution in distributions:
        _load_method = DISTRIBUTIONS[distribution]
        load_dicts = _load_method(no_queries=no_queries)

        for load_dict in load_dicts:
            load_conf = {}
            load_conf["loads"] = load_dict["loads"]
            load_conf["query_proportion"] = load_dict["query_proportion"]
            load_conf["majority_proportion"] = load_dict["majority_proportion"]
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
    backup_path = f"./data/backup/{dataset}"

    if os.path.exists(db_path):
        os.remove(db_path)
        # print(f"Removed db at path {db_path}")

    with duckdb.connect(db_path) as con:
        con.execute("SET default_block_size = '16384'")
        con.execute(f"IMPORT DATABASE '{backup_path}';")


def _create_connection(dataset: str, test: str) -> tuple[duckdb.DuckDBPyConnection, str]:
    original_db_path = f"./data/db/{dataset}.duckdb"
    copy_db_path = f"./data/db/{dataset}_{test}.db"

    # Remove any old db
    if os.path.exists(copy_db_path):
        os.remove(copy_db_path)
        # print(f"Removed db at path {copy_db_path}")

    # Copy original db
    shutil.copy(original_db_path, copy_db_path)

    # Reconnect to db
    con = duckdb.connect(copy_db_path)
    con.execute("SET default_block_size = '16384'")

    con.execute("CHECKPOINT;")
    db_size = os.path.getsize(copy_db_path)
    # print(f"Fresh database size: {db_size/1024/1024:.6f} MB")

    return con, copy_db_path


def _perform_test(
    con: duckdb.DuckDBPyConnection,
    dataset: str,
    test: str,
    load: list[str],
    run_test: bool
):

    times = []
    queries = []
    total_time = 0

    for i, query_name in enumerate(load):
        if run_test:
            execution_time = -1
        else:
            with open(f"./queries/{dataset}/{query_name}.sql", 'r') as f:
                query = f.read()
            start_time = time.perf_counter()
            _ = con.execute(query).fetchdf()
            end_time = time.perf_counter()
            execution_time = end_time - start_time

        times.append({"q": query_name, test: execution_time})
        total_time += execution_time

    print(f"Total time taken for test {test}: {total_time}")

    times_df = pd.DataFrame(columns=["q", test], data=times)
    return times_df, total_time


def main():
    dataset = "tpch"

    if not os.path.exists(f"./results/{dataset}/{TEST_TIME_STRING}"):
        os.mkdir(f"./results/{dataset}/{TEST_TIME_STRING}")

    config = DATASETS[dataset]
    standard_tests = config["standard_tests"]
    column_map = config["column_map"]

    distributions = ["numerical"]  # TODO input-based

    load_types = _generate_loads(
        distributions=distributions, no_queries=config["no_queries"])

    # Make sure query frequency exists
    analyze_queries(data_set=dataset)

    # Get field distribution for each query
    field_distribution = pd.read_csv(
        f'./results/{dataset}/field_distribution_{dataset}.csv')

    # Create fresh db
    _create_fresh_db(dataset=dataset)

    field_distribution.set_index(keys=['query'])

    meta_results = []

    for distribution in distributions:

        # Get the simulated loads
        load_type = load_types[distribution]

        for load_setup in load_type:

            loads = load_setup["loads"]
            query_proportion = load_setup["query_proportion"]
            majority_proportion = load_setup["majority_proportion"]

            for load_no, load in enumerate(loads):
                t_time = time.perf_counter()

                times_df = pd.DataFrame(columns=["q"], data=load)

                tests = copy.deepcopy(standard_tests)

                field_frequency = _calculate_field_frequency(
                    load=load, field_distribution=field_distribution)

                # Keep only fields with frequency above thresholds
                for treshold in MATERIALIZE_TRESHOLDS:
                    frequent_fields = field_frequency[field_frequency >=
                                                      NO_QUERIES * treshold]

                    tests[f"q{query_proportion}|m{majority_proportion}|t{treshold}"] = {
                        "materialization": frequent_fields.index.tolist()
                    }

                for test, setup in tests.items():
                    db_connection, db_path = _create_connection(
                        dataset=dataset, test=test)

                    materialize_columns = setup["materialization"]
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
                    _times_df, total_time = _perform_test(
                        con=db_connection,
                        dataset=dataset,
                        test=test,
                        load=load,
                        run_test=len(
                            materialize_columns) == 0 and test != 'no_materialization'
                    )

                    times_df = pd.merge(times_df, _times_df, on="q")

                    # Close db connection
                    db_connection.execute("CHECKPOINT;")
                    db_connection.close()

                    db_size = os.path.getsize(db_path)

                    meta_results.append({
                        "Test": test,
                        "Time taken": time_taken,
                        "Materialization": materialize_columns,
                        "Total query time": total_time,
                        "DB size": db_size
                    })

                    os.remove(db_path)

                print(
                    f"TOTAL TIME FOR ONE LOAD: {time.perf_counter() - t_time}")

                meta_results_df = pd.DataFrame(meta_results)
                meta_results_df.to_csv(
                    f"./results/{dataset}/meta_results.csv", index=False)

                times_df.to_csv(
                    f"./results/{dataset}/{datetime()}/q{query_proportion}|m{majority_proportion}|l{load_no}.csv")
                # f"./results/{dataset}/{datetime()}/q{query_proportion}|m{majority_proportion}|l{load_no}.csv", index=False)


if __name__ == "__main__":
    t = time.perf_counter()

    main()

    print(f"Total time for load tests: {time.perf_counter() - t}")
