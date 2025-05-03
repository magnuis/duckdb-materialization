import os
import random
import time
import shutil
from collections import defaultdict
from datetime import datetime

from queries.query import Query


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
            },
            #     {
            #         "scale_factor": 2,
            #         "dir": "tpch_bigger"
            #     },
            #     {
            #         "scale_factor": 4,
            #         "dir": "tpch_bigbigger"
            #     }
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

CLEAN_UP_FILES = set()


def _prepare_dirs(dataset: str):
    if not os.path.exists("./results/verify-datasize-irrelevance"):
        os.mkdir("./results/verify-datasize-irrelevance")
    if not os.path.exists(f"./results/verify-datasize-irrelevance/{dataset}"):
        os.mkdir(f"./results/verify-datasize-irrelevance/{dataset}")
    if not os.path.exists(f"./results/verify-datasize-irrelevance/{dataset}/{TEST_TIME_STRING}"):
        os.mkdir(
            f"./results/verify-datasize-irrelevance/{dataset}/{TEST_TIME_STRING}")


def _generate_materializations(
        queries: dict[str, Query],
):

    def _check_duplicates(old: list[str], new: list[str]):
        if len(old) == 0:
            return True
        for l in old:
            if sorted(l) == sorted(new):
                return False
        return True

    materializations = dict()

    # Generate test setup for the queries
    for query_name, query_obj in queries.items():

        query_setup = defaultdict(list)

        columns_in_query = query_obj.columns_used()

        for treshold in TRESHOLDS_TO_MATERIALIZE:
            no_to_materialize = int(len(columns_in_query) * treshold)

            # Must materialize at least one
            no_to_materialize = max(no_to_materialize, 1)

            seed = 0

            for _ in range(TESTS_PER_TRESHOLD):
                set_is_valid = False

                while not set_is_valid:
                    seed += 1
                    r = random.Random(x=seed)

                    cols_to_materialize = r.sample(
                        columns_in_query, no_to_materialize
                    )

                    set_is_valid = _check_duplicates(
                        old=query_setup[treshold], new=cols_to_materialize)

                    assert seed < 1000

                query_setup[treshold].append(cols_to_materialize)
        materializations[query_name] = query_setup

    return materializations


def _create_connection(db_dir: str) -> tuple[duckdb.DuckDBPyConnection, str]:
    original_db_path = f"./data/db/{db_dir}.duckdb"
    copy_db_path = f"./data/db/{db_dir}_test.db"

    CLEAN_UP_FILES.add(copy_db_path)

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


def _clean_up():
    print(CLEAN_UP_FILES)
    for file in list(CLEAN_UP_FILES):
        try:
            os.remove(file)
            print(f"Deleted file {file}")
        except FileNotFoundError:
            print(f"No file at path {file}")


def _perform_tests(
        query_name: str,
        query_object: Query,
        db_dir: str,
        materializations: dict,
        dataset: str
) -> pd.DataFrame:
    config = DATASETS[dataset]
    column_map: dict = config["column_map"]

    rows = []  # list to collect row dictionaries
    iterations = 5

    # will hold the query result from the first materialization
    global_baseline_result = None

    for threshold, field_lists in materializations.items():
        for index, fields_list in enumerate(field_lists):

            iteration_time = time.perf_counter()

            # Initialize a row dictionary for this test
            row = {"Query": query_name, "Param": threshold, "Replicate": index}

            # Create the field-materialization setup for this test
            fields = []
            for field, access_query in column_map.items():
                fields.append((field, access_query, field in fields_list))

            query = query_object.get_query(fields=fields)

            baseline_result = None  # to store the query result from the first iteration

            con, copy_con = _create_connection(db_dir=db_dir)
            prepare_database(con=con, fields=fields, include_print=False)

            execution_times = []

            # Execute the test iterations and record their times
            for i in range(iterations):
                start_time = time.perf_counter()
                result = con.execute(query).fetchdf()
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                execution_times.append(execution_time)

                if i == 0:
                    # Save the result of the first iteration as the baseline for this test.
                    baseline_result = result.copy()
                else:
                    # Check that subsequent iterations yield the same result.
                    if not baseline_result.equals(result):
                        raise ValueError(
                            f"Query results differ in iteration {i} for threshold {threshold}, replicate {index}!")
                row[f"Iteration {i}"] = execution_time

            # Compute average execution time over iterations 1 to 4
            avg_time = sum(execution_times[1:]) / (iterations - 1)
            row['Avg (last 4 runs)'] = avg_time

            row["No. materialized fields"] = len(fields_list)
            row["Materialized fields"] = fields_list

            rows.append(row)

            # Check consistency across all materializations:
            if global_baseline_result is None:
                global_baseline_result = baseline_result.copy()
            else:
                if not global_baseline_result.equals(baseline_result):
                    raise ValueError(
                        f"Query result for threshold {threshold}, replicate {index} differs from previous materializations!")

        print(
            f"[{query_name} t{threshold:.2f}] Time {(time.perf_counter() - iteration_time):.3f}")

    # Create the flat DataFrame directly from the rows list
    df_flat = pd.DataFrame(rows)

    return df_flat


def _create_fresh_db(db_dir: str):
    db_path = f"./data/db/{db_dir}.duckdb"
    backup_path = f"./data/backup/{db_dir}"

    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed db at path {db_path}")

    with duckdb.connect(db_path) as con:
        con.execute("SET default_block_size = '16384'")
        con.execute(f"IMPORT DATABASE '{backup_path}';")

    CLEAN_UP_FILES.add(db_path)


def main():

    dataset = "tpch"

    # Make sure directories exists
    _prepare_dirs(dataset=dataset)

    # Extract setup metadata
    config = DATASETS[dataset]
    queries: list = config["queries"]
    column_map: dict = config["column_map"]
    db_backups: dict = config["db_backups"]

    # Generate random materializations for each query
    materializations = _generate_materializations(queries=queries)

    query_results_dfs = dict()  # Query results
    meta_results = []

    for db in db_backups:
        scale_factor = db["scale_factor"]
        db_dir = db["dir"]
        _create_fresh_db(db_dir=db_dir)

        query_results_list = []

        for query_name, strategies in materializations.items():
            test_time = time.perf_counter()
            query_results = _perform_tests(
                query_name=query_name,
                query_object=queries[query_name],
                materializations=strategies,
                db_dir=db_dir,
                dataset=dataset
            )
            query_results_list.append(query_results)
            print(
                f"Finishied query {query_name}, scale factor {scale_factor} in time {int(time.perf_counter() - test_time)} seconds")

        merged_query_results = pd.concat(query_results_list, ignore_index=True)

        merged_query_results["Scale_factor"] = scale_factor

        # Reorder columns so that 'scale_factor' is the second column
        cols = merged_query_results.columns.tolist()
        cols.remove("Scale_factor")
        cols.insert(1, "Scale_factor")
        merged_query_results = merged_query_results[cols]

        meta_results.append(merged_query_results)

        print(f"!! Finished with scale factor {scale_factor}")
        print("-------------------------------------------")

    # Merge all the DataFrames from the db_backups loop into a final shared DataFrame
    final_shared_df = pd.concat(meta_results, ignore_index=True)
    final_shared_df.to_csv(
        f"./results/verify-datasize-irrelevance/{dataset}/{TEST_TIME_STRING}/meta_data.csv")


if __name__ == "__main__":
    main()
    _clean_up()
