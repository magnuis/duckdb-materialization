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

    _create_view(con=con, fields=fields)
    return time_taken


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
    all_materialized = True
    materialize_fields = []
    for field, query, materialize in fields:
        # Drop column if it exists
        con.execute(f"ALTER TABLE test_table DROP COLUMN IF EXISTS {field};")

        if materialize:
            alter_query += f"ALTER TABLE test_table ADD {field} {query['type']};"
            update_query += f"{field} = {query['query']}, "
            materialize_fields.append(field)

        materialized |= materialize
        all_materialized &= materialize

    if materialized:
        update_query = update_query[:-2] + ';'
        query = alter_query + " " + update_query

        if all_materialized:
            pass
            # query += "ALTER TABLE test_table DROP COLUMN IF EXISTS raw_json;"
        query += " END TRANSACTION;"

        start_time = time()
        con.execute("CHECKPOINT;")

        _print_db_size(con=con)
        con.execute(query)
        con.execute("CHECKPOINT;")

        if all_materialized:
            con.execute("CHECKPOINT;")
            _print_db_size(con=con)
            con.execute(
                "ALTER TABLE test_table DROP COLUMN IF EXISTS raw_json;")
            con.execute("CHECKPOINT;")

            _print_db_size(con=con)

        end_time = time()
        time_taken = end_time - start_time
    # print(f"Time taken to alter table: {time_taken} seconds")

    print('------------------------------')
    print(f"Materialized {len(materialize_fields)} fields")
    print(materialize_fields)
    return time_taken


def _create_view(con: duckdb.DuckDBPyConnection, fields: list[tuple[str, dict, bool]]):
    """
    Create view for the provided fields

    Parameters
    ----------
    fields : list[tuple[str, dict, bool]]
        List of tuples of field name, json extraction query, and materialized status
    """
    view_query = "DROP VIEW IF EXISTS test_view; CREATE VIEW test_view AS SELECT"
    json_view = "CAST('{' || rtrim("
    all_materialized = True
    for field, query, materialize in fields:
        if materialize:
            view_query += f""" {field},"""
            json_view += f"""COALESCE(CASE WHEN {field} IS NOT NULL THEN '"{field}": ' || to_json({field}) || ', ' ELSE '' END, '') ||"""
        else:
            view_query += f" {query['query']} AS {field},"
            all_materialized = False
    # if all_materialized:
    #     view_query += json_view + "'', ', ') || '}' AS JSON) AS raw_json"
    # else:
    #     view_query += " raw_json"

    view_query += " FROM test_table;"

    con.execute("CHECKPOINT;")
    con.execute(view_query)
    _print_db_size(con=con)
    con.execute("CHECKPOINT;")
    _print_db_size(con=con)


def _check_db_size(con: duckdb.DuckDBPyConnection, dataset: str):

    temp_db = f"./data/db/temp_{dataset}.db"

    if os.path.exists(temp_db):
        # print("Removed temp_db")
        os.remove(temp_db)

    con.execute(f"""
        ATTACH '{temp_db}' AS temp_db;
        COPY FROM DATABASE original_db TO temp_db;
    """)

    con.execute("CHECKPOINT temp_db;")

    cols = con.execute('DESCRIBE original_db.test_table').fetchall()
    for col in cols:
        print(col)

    con.execute('DETACH temp_db;')

    db_size = os.path.getsize(f"./data/db/temp_{dataset}.db")

    os.remove(temp_db)

    return db_size


def _print_db_size(con: duckdb.DuckDBPyConnection):
    print(con.execute(
        "CALL pragma_database_size();").fetch_df())
