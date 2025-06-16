# pylint: disable=E0401
import os
import time
import duckdb  # type: ignore


def prepare_database(con: duckdb.DuckDBPyConnection, fields: list[tuple[str, dict, bool]], include_print: bool = True) -> float:
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
    time_taken = _alter_table(con=con, fields=fields,
                              include_print=include_print)

    # _create_view(con=con, fields=fields)

    con.execute("ANALYZE;")
    return time_taken


def _alter_table(con: duckdb.DuckDBPyConnection, fields: list[tuple[str, dict, bool]], include_print: bool = True) -> float:
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

    materialized = False
    all_materialized = True
    materialize_fields = {}
    col_types = {}

    for field, query, materialize in fields:
        # Drop column if it exists
        con.execute(f"ALTER TABLE test_table DROP COLUMN IF EXISTS {field};")

        if materialize:
            materialize_fields[field] = query['type']
            col_types[field] = query['type']

        materialized |= materialize
        all_materialized &= materialize

    # Do nothing if not fields to materialize
    if len(materialize_fields.keys()) == 0:
        if include_print:
            print("No fields to materialize.")
        return time_taken

    # Build ALTER statements
    alter_parts = []
    for field in materialize_fields.keys():
        alter_parts.append(
            f"ALTER TABLE test_table ADD COLUMN {field} {col_types[field]};"
        )
    alter_sql = "\n".join(alter_parts)

    stringified_fields = [f"'{field}'" for field in materialize_fields.keys()]
    cte_sql = f"""
    WITH extracted AS (
        SELECT rowid, json_extract_string(raw_json, [{", ".join(stringified_fields)}]) AS json_arr
        FROM test_table
    )
    """

    update_assigns = []
    for idx, (field, data_type) in enumerate(materialize_fields.items(), start=1):
        update_assigns.append(
            f"{field} = extracted.json_arr[{idx}]::{data_type}")

    update_sql = f"""
        UPDATE test_table SET
            {', '.join(update_assigns)}
        FROM extracted
        WHERE test_table.rowid = extracted.rowid;
    """
    full_sql = "BEGIN TRANSACTION; " + alter_sql + \
        cte_sql + update_sql + "END TRANSACTION;"

    start_time = time.perf_counter()

    con.execute(full_sql)
    end_time = time.perf_counter()
    time_taken = end_time - start_time

    con.execute("CHECKPOINT;")

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
            view_query += f" {query['access']} AS {field},"
            all_materialized = False
    # if all_materialized:
    #     view_query += json_view + "'', ', ') || '}' AS JSON) AS raw_json"
    # else:
    #     view_query += " raw_json"

    view_query += " FROM test_table;"

    con.execute("CHECKPOINT;")
    con.execute(view_query)
    con.execute("CHECKPOINT;")
    print(view_query)


def _check_db_size(con: duckdb.DuckDBPyConnection, dataset: str):

    temp_db = os.curdir + f"/data/db/temp_{dataset}.db"

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

    db_size = os.path.getsize(os.curdir + f"/data/db/temp_{dataset}.db")

    os.remove(temp_db)

    return db_size


def get_db_size(con: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    """
    Get database size information

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        DuckDB connection

    Returns
    -------
    tuple[int, int, int]
        (used_blocks, block_size, total_size_in_bytes)
    """
    result = con.execute("CALL pragma_database_size();").fetchone()
    # database_name(0) database_size(1) block_size(2) total_blocks(3) used_blocks(4) free_blocks(5)...
    block_size = result[2]
    used_blocks = result[4]
    total_size = used_blocks * block_size
    return (used_blocks, block_size, total_size)


def _print_db_size(con: duckdb.DuckDBPyConnection):
    used_blocks, block_size, total_size = get_db_size(con)
    print(
        f"Database size: {used_blocks} blocks * {block_size} bytes = {total_size} bytes")
