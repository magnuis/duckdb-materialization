import os
from time import time
import duckdb


def prepare_database(con: duckdb.DuckDBPyConnection, dataset: str, fields: list[tuple[str, dict, bool]]) -> float:
    # def prepare_database(con: duckdb.DuckDBPyConnection, fields: dict[str, bool]):
    """
    Prepare the database by materializing the correct fields and creating the view

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        DuckDB connection
    fields : list[tuple[str, dict, bool]]
        List of tuples of field name, json extraction query, and materialized status
    """
    time_taken = _alter_table(con=con, fields=fields)

    _create_view_query(con=con, fields=fields)
    con.execute("CHECKPOINT;")
    con.execute("VACUUM;")
    db_size = _check_db_size(con=con, dataset=dataset)
    print(f"Database size after: {db_size/1024/1024:.6f} MB")
    return time_taken, db_size


def _alter_table(con: duckdb.DuckDBPyConnection, fields: list[tuple[str, dict, bool]]) -> float:
    """
    Alter the table for the given field

    If the field should be materialized, and does not exist, create it.
    If the field should not be materialized but is present, drop column

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        DuckDB connection
    fields : list[tuple[str, dict, bool]]
        List of tuples of field name, json extraction query, and materialized status

    Returns
    -------
    time_taken : float
        Time taken to alter table (not including dropping columns)
    """

    time_taken = 0

    alter_query = "BEGIN TRANSACTION; "
    update_query = "UPDATE test_table SET "

    materialized = False
    for field, query, materialize in fields:
        # Drop column if it exists
        con.execute(f"ALTER TABLE test_table DROP COLUMN IF EXISTS {field};")

        if materialize:
            alter_query += f"ALTER TABLE test_table ADD {field} {query['type']};"
            update_query += f"{field} = {query['query']}, "

        materialized |= materialize

    if materialized:
        update_query = update_query[:-2] + ';'

        query = alter_query + " " + update_query + " END TRANSACTION;"

        start_time = time()
        # print(query)
        con.execute(query)

        end_time = time()
        time_taken = end_time - start_time
    print(f"Time taken to alter table: {time_taken} seconds")

    return time_taken


def _create_view_query(con: duckdb.DuckDBPyConnection, fields: list[tuple[str, dict, bool]]):
    """
    Prepare create view query for provided fields

    Parameters
    ----------
    fields : list[tuple[str, dict, bool]]
        List of tuples of field name, json extraction query, and materialized status
    """

    view_query = "DROP VIEW IF EXISTS test_view; CREATE VIEW test_view AS SELECT"

    for field, query, materialize in fields:
        if materialize:
            view_query += f" {field},"
        else:
            view_query += f" {query['query']} AS {field},"

    view_query += " FROM test_table;"

    con.execute(view_query)


def _check_db_size(con: duckdb.DuckDBPyConnection, dataset: str):

    temp_db = f"./data/db/temp_{dataset}.db"

    con.execute(f"""
        ATTACH '{temp_db}' AS temp_db;
        COPY FROM DATABASE original_db TO temp_db;
    """)

    con.execute("CHECKPOINT temp_db;")

    db_size = os.path.getsize(f"./data/db/temp_{dataset}.db")

    print(f"Database size after: {db_size/1024/1024:.6f} MB")
    con.execute('DETACH temp_db;')

    os.remove(temp_db)

    return db_size
