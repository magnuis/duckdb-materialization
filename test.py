import duckdb
import os
import sys

# Database path
DB_PATH = 'data/db/tpch.db'

def execute_query(query_path: str):
    """
    Execute a SQL query from a file and return the results.
    
    Parameters
    ----------
    query_path : str
        Path to the SQL query file
    
    Returns
    -------
    list
        Query results
    """
    if not os.path.exists(query_path):
        raise FileNotFoundError(f"Query file not found: {query_path}")
    
    # Connect to the database
    con = duckdb.connect(DB_PATH)
    
    try:
        # Read and execute the query
        with open(query_path, 'r') as f:
            query = f.read()
        
        # Execute the query and fetch results
        result = con.execute(query).fetchall()
        
        # Print results
        print(f"Query Results for {query_path}:")
        for row in result:
            print(row)
            
        return result
            
    finally:
        # Close the connection
        con.close()

if __name__ == "__main__":
    # Default to q0.sql if no argument is provided
    query_path = sys.argv[1] if len(sys.argv) > 1 else 'queries/tpch/q0.sql'
    execute_query(query_path)