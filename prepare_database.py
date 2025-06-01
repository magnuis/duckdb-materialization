import os
from time import time
import duckdb


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

    con.execute("ANALYZE")
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

    alter_query = "BEGIN TRANSACTION; "
    # update_query = "UPDATE test_table SET "
    # cte_query = "WITH extracted AS (SELECT rowid, json_extract_string(raw_json, ["

    materialized = False
    all_materialized = True
    materialize_fields = []
    # cte_list_index = 1
    for field, query, materialize in fields:
        # Drop column if it exists
        con.execute(f"ALTER TABLE test_table DROP COLUMN IF EXISTS {field};")

        if materialize:
            data_type = query['type']
            alter_query += f"ALTER TABLE test_table ADD COLUMN {field} {data_type}; "
            alter_query += f"UPDATE test_table SET {field} = {query['access']}; "
            # cte_query += f"'{field}', "
            # if data_type == 'VARCHAR':
            #     update_query += f"{field} = e.extracted_list[{cte_list_index}], "
            # else:
            #     update_query += f"{field} = e.extracted_list[{cte_list_index}]::{data_type}, "
            # materialize_fields.append(field)
            # cte_list_index += 1

        materialized |= materialize
        all_materialized &= materialize

    # Remove the last whitespace and comme
    # cte_query = cte_query[:-2]
    # End CTE satement
    # cte_query += "]) AS extracted_list FROM test_table)"
    if materialized:

        # update_query = update_query[:-2] + \
        #     ' FROM extracted AS e WHERE e.rowid = test_table.rowid;'
        # query = alter_query + " " + cte_query + " " + update_query

        if all_materialized:
            pass
            # query += "ALTER TABLE test_table DROP COLUMN IF EXISTS raw_json;"
        # query += " COMMIT;"
        print(alter_query)

        start_time = time()

        con.execute(alter_query + 'COMMIT;')
        # con.execute(query)
        # print(query)
        con.execute("CHECKPOINT;")

        # if all_materialized:
        #     con.execute(
        #         "ALTER TABLE test_table DROP COLUMN IF EXISTS raw_json;")
        #     con.execute("CHECKPOINT;")

        end_time = time()
        time_taken = end_time - start_time
    # print(f"Time taken to alter table: {time_taken} seconds")

    # if include_print:
    #     print('------------------------------')
        print(
            f"Materialized {len(materialize_fields)} fields in time {time_taken:.3f}")
    #     print(materialize_fields)
        print(query)
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
