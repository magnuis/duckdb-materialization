import shutil
import os
import pandas as pd
import string
import time
from typing import List, Dict
from datetime import datetime
import random
import duckdb

from prepare_database import prepare_database, get_db_size
import testing.tpch.setup as tpch_setup
from queries.query import Query

CHARS = string.ascii_letters + string.digits + " "
LINES_TO_INSERT = 5000

BASE_PATH = os.curdir
PATHS_TO_REMOVE = set()

DATASETS = {
    "tpch": {
        "queries": tpch_setup.QUERIES,
        "standard_tests": tpch_setup.STANDARD_SETUPS,
        "column_map": tpch_setup.COLUMN_MAP,
        "no_queries": len(tpch_setup.QUERIES),
        "results_path": BASE_PATH + "/results/load-based-N-fields/tpch/2025-03-26-15H"
    }
}

TEST_TIME_STRING = f"{datetime.now().date()}-{datetime.now().hour}H"


def _create_fresh_db(dataset: str):
    db_path = BASE_PATH + f"/data/db/{dataset}.duckdb"
    backup_path = BASE_PATH + f"/data/backup/{dataset}_bigger"

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

    PATHS_TO_REMOVE.add(copy_db_path)

    # Reconnect to db
    con = duckdb.connect(copy_db_path)

    con.execute("CHECKPOINT;")

    return con, copy_db_path


def _generate_json(seed: int, dataset: str, orig_table: str = None):
    random_gen = random.Random()
    random_gen.seed(seed)

    def _rand_int(min: int = 0, max: int = 1000000):
        return random_gen.randint(min, max)

    def _rand_float():
        return _rand_int() + random_gen.random()

    def _rand_varchar(str_length: int = None):
        if str_length is None:
            str_length = _rand_int(min=10, max=50)
        return "".join(random_gen.choice(CHARS) for _ in range(str_length))

    def _rand_date():
        return f"{_rand_int(min=1990, max=2025)}-{_rand_int(min=1, max=12):02d}-{_rand_int(min=1, max=28):02d}"

    table_structures: Dict[str, Dict] = {
        "tpch": {
            "c": {

                "c_custkey": _rand_int(),
                "c_nationkey": _rand_int(),
                "c_mktsegment": _rand_varchar(),
                "c_name": _rand_varchar(),
                "c_phone": _rand_varchar(),
                "c_address": _rand_varchar(),
                "c_comment": _rand_varchar(str_length=1000),
                "c_acctbal": _rand_float()
            },
            "l": {
                "l_orderkey": _rand_int(),
                "l_partkey": _rand_int(),
                "l_suppkey": _rand_int(),
                "l_linenumber": _rand_int(),
                "l_quantity": _rand_float(),
                "l_extendedprice": _rand_float(),
                "l_discount": _rand_float(),
                "l_tax": _rand_float(),
                "l_returnflag": _rand_varchar(str_length=1),
                "l_linestatus": _rand_varchar(str_length=1),
                "l_shipdate": _rand_date(),
                "l_commitdate": _rand_date(),
                "l_receiptdate": _rand_date(),
                "l_shipinstruct": _rand_varchar(),
                "l_shipmode": _rand_varchar(),
                "l_comment": _rand_varchar(str_length=1000),
            },
            "n": {
                "n_nationkey": _rand_int(),
                "n_regionkey": _rand_int(),
                "n_name": _rand_varchar(),
            },
            "o": {
                "o_orderdate": _rand_date(),
                "o_totalprice": _rand_float(),
                "o_shippriority": _rand_int(),
                "o_custkey": _rand_int(),
                "o_orderkey": _rand_int(),
                "o_orderpriority": _rand_varchar(),
                "o_comment": _rand_varchar(str_length=1000),
                "o_orderstatus": _rand_varchar(str_length=1)
            },
            "p": {
                "p_type": _rand_varchar(),
                "p_name": _rand_varchar(),
                "p_partkey": _rand_int(),
                "p_size": _rand_int(),
                "p_mfgr": _rand_varchar(),
                "p_brand": _rand_varchar(),
                "p_container": _rand_varchar(),
            },
            "ps":
                {"ps_partkey": _rand_int(),
                 "ps_suppkey": _rand_int(),
                 "ps_availqty": _rand_int(),
                 "ps_supplycost": _rand_int(),
                 "ps_comment": _rand_varchar(str_length=1000),
                 },
            "r": {
                "r_regionkey": _rand_int(),
                "r_comment": _rand_varchar(str_length=1000),
                "r_name": _rand_varchar()
            },
            "s": {
                "s_suppkey": _rand_int(),
                "s_name": _rand_varchar(),
                "s_address": _rand_varchar(),
                "s_nationkey": _rand_int(),
                "s_phone": _rand_varchar(),
                "s_acctbal": _rand_float(),
                "s_comment": _rand_varchar(str_length=1000),
            },
        }
    }

    if orig_table is None:
        tables = list(table_structures[dataset].keys())
        orig_table = random_gen.choice(tables)

    return table_structures[dataset][orig_table]


def _update_query(fields: list[tuple[str, dict, bool]], row_id: int):
    materialize_fields = {}
    col_types = {}

    has_materialization = False

    for field, query, materialize in fields:
        if materialize:
            materialize_fields[field] = query['type']
            col_types[field] = query['type']
            has_materialization = True
    if not has_materialization:
        return None

    stringified_fields = [f"'{field}'" for field in materialize_fields.keys()]

    update_assigns = []
    for idx, (field, data_type) in enumerate(materialize_fields.items(), start=1):
        update_assigns.append(
            f"{field} = extracted.json_arr[{idx}]::{data_type}")

    update_query = f"""
    WITH extracted AS (
        SELECT json_extract_string(raw_json, [{", ".join(stringified_fields)}]) AS json_arr
        FROM test_table
        WHERE rowid = {row_id}
    )
    UPDATE test_table SET
        {', '.join(update_assigns)}
    FROM extracted
    WHERE test_table.rowid = {row_id};
    """
    return update_query


def _perform_test(
    con: duckdb.DuckDBPyConnection,
    fields: list[tuple[str, dict, bool]],
    dataset: str = "tpch",
):
    execution_time = 0
    for i in range(LINES_TO_INSERT):
        data_row = _generate_json(seed=i, dataset=dataset)

        # Insert new row
        insert_query = f"INSERT INTO test_table (raw_json) VALUES ({data_row});"
        start_time = time.perf_counter()
        con.execute(insert_query)
        end_time = time.perf_counter()
        execution_time += end_time - start_time

        # Get the id of the last inserted row
        row_id = con.execute(
            "SELECT LAST(rowid) FROM test_table;").fetchone()[0]

        # Update columns with json data
        update_query = _update_query(fields=fields, row_id=row_id)
        if update_query is not None:
            start_time = time.perf_counter()
            con.execute(update_query)
            end_time = time.perf_counter()
            execution_time += end_time - start_time

    return execution_time


def _clean_up():
    for file in list(PATHS_TO_REMOVE):
        try:
            os.remove(file)
            print(f"Deleted file {file}")
        except FileNotFoundError:
            print(f"No file at path {file}")


def main():
    dataset = "tpch"

    config = DATASETS[dataset]
    column_map: dict = config["column_map"]

    results_path = config["results_path"]

    loads_df = pd.read_csv(results_path + "/meta_results.csv")
    loads_df = loads_df[["Load", "Test", "Materialization"]]

    timed_loads = dict()

    results_df = pd.DataFrame(
        columns=["Load", "Test", "Materialization", "Write time", "DB Size Before", "DB Size After", "Materialization Time"])

    # Create fresh db
    _create_fresh_db(dataset=dataset)
    # Create new db connection
    db_connection, db_path = _create_connection(dataset=dataset)

    prev_load = 0
    for loads_df_row in loads_df.itertuples():
        # for field in list(column_map.keys()) + [None, None, None]:
        # if field is not None:
        # continue

        load_time = time.time()

        # load_no = 11
        # test_name: str = field if field is not None else "no_materialization"
        # materialized_fields_list = [field] if field is not None else []
        load_no: str = loads_df_row[1]
        test_name: str = loads_df_row[2]
        materialized_fields: str = loads_df_row[3]

        materialized_fields_list = materialized_fields.replace(
            "[", "").replace("]", "").replace(" ", "").replace("'", "").split(",")

        if '' in materialized_fields_list:
            materialized_fields_list.remove('')

        # No materialization
        # if False:
            # pass
        if len(materialized_fields_list) <= 0:
            write_time = 0
            db_size_before = 0
            db_size_after = 0
            prepare_time = 0

        # Don't repeat tests
        elif materialized_fields in timed_loads:
            print(f"{materialized_fields} already encountered")
            write_time = timed_loads[materialized_fields]["write_time"]
            db_size_before = timed_loads[materialized_fields]["db_size_before"]
            db_size_after = timed_loads[materialized_fields]["db_size_after"]
            prepare_time = timed_loads[materialized_fields]["prepare_time"]

        else:

            # Create the field-materialization setup for this test
            fields = []
            for field, access_query in column_map.items():
                fields.append(
                    (field, access_query, field in materialized_fields_list))

            # Get db size before materialization
            db_size_before = get_db_size(db_connection)
            # Prepare database
            prepare_time = prepare_database(con=db_connection, fields=fields)

            # Get db size before materialization
            # FIXME @herman3h se over og se om dette er gjort riktig
            db_size_after = get_db_size(db_connection)

            # Run test
            write_time = _perform_test(
                con=db_connection, fields=fields, dataset=dataset)
            print(
                f"Time taken to write {materialized_fields_list}: {write_time}")

            db_connection.execute("CHECKPOINT;")

            timed_loads[materialized_fields] = {
                #     "write_time": write_time, "db_size": db_size, "prepare_time": prepare_time}
                # timed_loads[str(materialized_fields_list)] = {
                "write_time": write_time, "db_size_before": db_size_before, "db_size_after": db_size_after, "prepare_time": prepare_time}

        results_df = pd.concat([
            results_df,
            pd.DataFrame([{
                "Load": load_no,
                "Test": test_name,
                "Materialization": materialized_fields_list,
                "Write time": write_time,
                "DB Size Before": db_size_before,
                "DB Size After": db_size_after,
                "Materialization Time": prepare_time
                # TODO add other relevant db sizes
            }])
        ], ignore_index=True)

        if prev_load != load_no:
            print(f"time taken for load {load_no}: {time.time() - load_time}")
            load_time = time.time()
            prev_load = load_no

    # Close connection
    db_connection.close()
    # Write results back to `results_path`
    results_df.to_csv(results_path + "/write_times.csv")


if __name__ == "__main__":
    t = time.time()
    main()

    _clean_up()

    print(f"Total time: {time.time() - t}s")
