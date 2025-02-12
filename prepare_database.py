import duckdb

def prepare_database(con: duckdb.DuckDBPyConnection, fields: list[tuple[str, dict, bool]]):
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
    changed = False
    for field, query, materialize in fields:
        changed |= _alter_table(con=con, field=field, query=query, materialize=materialize)


    query = create_view_query(fields=fields)
    con.execute(query)

def _alter_table(con: duckdb.DuckDBPyConnection, field: str, query: dict, materialize: bool):
    """
    Alter the table for the given field

    If the field should be materialized, and does not exist, create it.
    If the field should not be materialized but is present, drop column
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        DuckDB connection
    field : str
        Name of the field
    query : str
        Query to extract JSON value for field
    materialize : bool
        Whether to materialize the table
    
    Returns
    -------
    bool
        True if the field was altered, False otherwise
    """
    # Check if the field exists in db
    result = con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'test_table' AND column_name = '{field}'").fetchall()
    field_exists = len(result) > 0



    if materialize and not field_exists:
        # Create the column if it should be materialized but doesn't exist
        con.execute(f"ALTER TABLE test_table ADD COLUMN {field} {query['type']};")
        # Update the column with the value from raw_json
        set_query = f"UPDATE test_table SET {field} = {query['query']};"
        con.execute(set_query)
        print(f"Materialized field {field}.")
        return True
    elif not materialize and field_exists:
        print(f"Un-materialized field {field}.")
        # Drop the column if it shouldn't be materialized but exists
        con.execute(f"ALTER TABLE test_table DROP COLUMN {field};")
        return True

    return False

def create_view_query(fields: list[tuple[str, dict, bool]]):
    """
    Prepare create view query for provided fields
    
    Parameters
    ----------
    fields : list[tuple[str, dict, bool]]
        List of tuples of field name, json extraction query, and materialized status

    Returns
    -------
    str
        The created `create view query`
    """

    view_query = "DROP VIEW test_view; CREATE VIEW test_view AS SELECT"

    for field, query, materialize in fields:
        if materialize:
            view_query += f" {field},"
        else:
            view_query += f" {query['query']} AS {field},"
    
    view_query += " FROM test_table;"

    print(view_query)
    return view_query





