import shutil
import tempfile
from multiprocessing import Process
import time
from datetime import datetime
import argparse
import os
import json


TPCH_ORIGINAL_DATA_PATH = './data'
OUTPUT_DATA_PATH = '../data'
TPCH_OUTPUT_DATA_PATH = '../data/tpch'

# Create data directory if not exists
if not os.path.isdir(OUTPUT_DATA_PATH):
    os.mkdir(OUTPUT_DATA_PATH)

if not os.path.isdir(TPCH_OUTPUT_DATA_PATH):
    os.mkdir(TPCH_OUTPUT_DATA_PATH)


# Define the mapping of table names to their column names and data types
table_definitions = {
    'customer': {
        'columns': [
            'c_custkey', 'c_name', 'c_address', 'c_nationkey',
            'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'
        ],
        'types': [
            int,       # c_custkey
            str,       # c_name
            str,       # c_address
            int,       # c_nationkey
            str,       # c_phone
            float,     # c_acctbal
            str,       # c_mktsegment
            str        # c_comment
        ]
    },
    'lineitem': {
        'columns': [
            'l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber',
            'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax',
            'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
            'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'
        ],
        'types': [
            int,       # l_orderkey
            int,       # l_partkey
            int,       # l_suppkey
            int,       # l_linenumber
            float,     # l_quantity
            float,     # l_extendedprice
            float,     # l_discount
            float,     # l_tax
            str,       # l_returnflag
            str,       # l_linestatus
            'date',    # l_shipdate
            'date',    # l_commitdate
            'date',    # l_receiptdate
            str,       # l_shipinstruct
            str,       # l_shipmode
            str        # l_comment
        ]
    },
    'nation': {
        'columns': [
            'n_nationkey', 'n_name', 'n_regionkey', 'n_comment'
        ],
        'types': [
            int,       # n_nationkey
            str,       # n_name
            int,       # n_regionkey
            str        # n_comment
        ]
    },
    'orders': {
        'columns': [
            'o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
            'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority',
            'o_comment'
        ],
        'types': [
            int,       # o_orderkey
            int,       # o_custkey
            str,       # o_orderstatus
            float,     # o_totalprice
            'date',    # o_orderdate
            str,       # o_orderpriority
            str,       # o_clerk
            int,       # o_shippriority
            str        # o_comment
        ]
    },
    'part': {
        'columns': [
            'p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type',
            'p_size', 'p_container', 'p_retailprice', 'p_comment'
        ],
        'types': [
            int,       # p_partkey
            str,       # p_name
            str,       # p_mfgr
            str,       # p_brand
            str,       # p_type
            int,       # p_size
            str,       # p_container
            float,     # p_retailprice
            str        # p_comment
        ]
    },
    'partsupp': {
        'columns': [
            'ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost',
            'ps_comment'
        ],
        'types': [
            int,       # ps_partkey
            int,       # ps_suppkey
            int,       # ps_availqty
            float,     # ps_supplycost
            str        # ps_comment
        ]
    },
    'region': {
        'columns': [
            'r_regionkey', 'r_name', 'r_comment'
        ],
        'types': [
            int,       # r_regionkey
            str,       # r_name
            str        # r_comment
        ]
    },
    'supplier': {
        'columns': [
            's_suppkey', 's_name', 's_address', 's_nationkey',
            's_phone', 's_acctbal', 's_comment'
        ],
        'types': [
            int,       # s_suppkey
            str,       # s_name
            str,       # s_address
            int,       # s_nationkey
            str,       # s_phone
            float,     # s_acctbal
            str        # s_comment
        ]
    }
}

# The list of tables to process
tables = list(table_definitions.keys())

# Define date format used in the data (e.g., 'YYYY-MM-DD')
DATE_FORMAT = '%Y-%m-%d'


def process_table(table, limit_rows, temp_dir):
    """
    Process a TPCH table by reading its .tbl file, converting each row to JSON, and writing the results to a temporary file.

    Parameters
    ----------
    table : str
        The name of the table to process. The function expects a file named `<table>.tbl` in the TPCH original data path.
    limit_rows : bool
        If True, limits the processing to the first 10 valid rows of the table.
    temp_dir : str
        Directory where the temporary JSON output file will be stored.

    Returns
    -------
    None
        Writes the processed rows as JSON documents to a temporary file and prints progress and warnings. Does not return a value.
    """

    print(f'\nStarting processing table: {table}')
    start_time = time.time()

    columns = table_definitions[table]['columns']
    types = table_definitions[table]['types']
    filename = f'{table}.tbl'

    file_path = os.path.join(TPCH_ORIGINAL_DATA_PATH, filename)

    # Check if the .tbl file exists
    if not os.path.exists(file_path):
        print(f'File {file_path} does not exist. Skipping table {table}.')
        return

    # Try to get total number of lines in file
    with open(file_path, 'r') as infile:
        total_lines = sum(1 for line in infile)
    print(f'Table {table} has {total_lines} lines.')

    row_count = 0  # Initialize row counter
    warning_count = 0  # Initialize warning counter

    temp_output_file = os.path.join(temp_dir, f'{table}_temp.json')

    # Open the .tbl file again to process
    with open(file_path, 'r') as infile, open(temp_output_file, 'w') as outfile:
        for line_number, line in enumerate(infile, start=1):
            # Break if limit is reached
            if limit_rows and row_count >= 10:
                print(f'Limit of 10 rows reached for table {table}.')
                break

            # Strip trailing whitespace and delimiter
            line = line.strip()
            if not line:
                continue  # Skip empty lines

            # Split the line by '|' and remove the last empty element
            values = line.split('|')[:-1]

            # Check for correct number of columns
            if len(values) != len(columns):
                print(
                    f'Warning: Line {line_number} in {filename} has {len(values)} values but expected {len(columns)}. Skipping this line.')
                warning_count += 1
                continue

            # Convert values to appropriate data types
            converted_values = []
            for i, (value, data_type) in enumerate(zip(values, types)):
                try:
                    if data_type == int:
                        converted_values.append(int(value))
                    elif data_type == float:
                        converted_values.append(float(value))
                    elif data_type == 'date':
                        # Convert date string to ISO format
                        date_obj = datetime.strptime(value, DATE_FORMAT)
                        converted_values.append(date_obj.strftime('%Y-%m-%d'))
                    else:  # Assume string
                        converted_values.append(value)
                except ValueError as e:
                    print(
                        f'Warning: Type conversion error on table {table}, line {line_number}, column {columns[i]}: {e}. Setting value to None.')
                    warning_count += 1
                    converted_values.append(None)  # or handle as appropriate

            # Map column names to their respective converted values
            row = dict(zip(columns, converted_values))

            # Write the JSON document to the temporary output file
            json.dump(row, outfile)
            outfile.write('\n')

            row_count += 1  # Increment row counter

            # Provide progress feedback every 1,000 rows
            if row_count % 10000 == 0:
                print(f'Processed {row_count} rows from table {table}.')

    end_time = time.time()
    print(
        f'Finished processing {row_count} rows from table {table} in {end_time - start_time:.2f} seconds.')
    if warning_count > 0:
        print(
            f'Encountered {warning_count} warnings while processing table {table}.')


def analyze_fields_frequency():
    """
    For each .tbl file available in TPCH_ORIGINAL_DATA_PATH, count the number of lines.
    Then, for each table, compute the fraction of documents that come from that table.
    Each field (column) belonging to the table is assigned that same fraction.
    The results are written as CSV to a file with columns 'column_name,frequency'.
    """
    table_line_counts = {}
    total_lines = 0

    # Iterate over all tables defined in table_definitions
    for table in tables:
        file_path = os.path.join(TPCH_ORIGINAL_DATA_PATH, f"{table}.tbl")
        if not os.path.exists(file_path):
            print(f"File {file_path} not found. Skipping table '{table}'.")
            continue

        # Count the number of lines in the table file
        with open(file_path, "r") as f:
            count = sum(1 for _ in f)
        table_line_counts[table] = count
        total_lines += count
        print(f"Table '{table}' has {count} lines.")

    if total_lines == 0:
        print("No lines found in any table. Frequency analysis will not be generated.")
        return

    # Prepare the frequency data for each field (column)
    frequency_entries = []
    for table, count in table_line_counts.items():
        fraction = round(count / total_lines, 2)
        # Get the list of columns for the table
        columns = table_definitions[table]['columns']
        types = table_definitions[table]['types']
        for i, col in enumerate(columns):
            frequency_entries.append((col, types[i].__name__, fraction))

    frequency_entries.sort(key=lambda x: x[2], reverse=True)

    # Write the frequency data to a CSV file
    csv_file_path = os.path.join(TPCH_OUTPUT_DATA_PATH, "fields_frequency.csv")
    try:
        with open(csv_file_path, "w", newline="") as csv_file:
            # Write header
            csv_file.write("column_name,type,frequency\n")
            # Write each column and its computed frequency
            for col, col_type, freq in frequency_entries:
                csv_file.write(f"{col},{col_type},{freq}\n")
        print(f"Field frequency analysis written to {csv_file_path}")
    except Exception as e:
        print(f"Error writing frequency CSV file: {e}")


def convert(limit_rows, field_frequency):
    total_start_time = time.time()
    output_file = os.path.join(
        TPCH_OUTPUT_DATA_PATH, 'tpch.json')

    if limit_rows:
        print('Limiting output to first 10 rows from each table.')

    # Create a temporary directory to store temporary output files
    temp_dir = tempfile.mkdtemp()
    print(f'Temporary files will be stored in {temp_dir}')

    processes = []

    # Start a process for each table
    for table in tables:
        p = Process(target=process_table, args=(table, limit_rows, temp_dir))
        p.start()
        processes.append(p)

    # Wait for all processes to finish
    for p in processes:
        p.join()

    # Merge all temporary files into the final output file
    print('\nMerging temporary files into the final output file...')
    with open(output_file, 'w') as outfile:
        for table in tables:
            temp_output_file = os.path.join(temp_dir, f'{table}_temp.json')
            if os.path.exists(temp_output_file):
                with open(temp_output_file, 'r') as infile:
                    shutil.copyfileobj(infile, outfile)

    # Clean up temporary directory
    shutil.rmtree(temp_dir)
    print(f'Temporary files have been deleted from {temp_dir}')

    total_end_time = time.time()
    print(
        f'\nConversion completed in {total_end_time - total_start_time:.2f} seconds.')
    print(f'JSON data is written to {output_file}.')

    if field_frequency:
        analyze_fields_frequency()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Convert TPC-H .tbl files to JSON with multiprocessing.')
    parser.add_argument('--limit', action='store_true',
                        help='Limit output to first 10 rows from each table.')
    # Optionally, add a flag to trigger frequency analysis
    parser.add_argument('--field_frequency', action='store_true',
                        help='Generate CSV file with field frequencies.')
    args = parser.parse_args()

    convert(limit_rows=args.limit, field_frequency=args.field_frequency)
