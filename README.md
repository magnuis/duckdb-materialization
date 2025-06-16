# duckdb-materialization
Welcome to the repo concerning our master's thesis! Below is th ebare minimum you will need in order to navigate through the repository - good luck!

## Setup
Requirements are found in `requirements.txt`.

## Queries
All queries have a `.py` file used for testing, and a corresponding `.sql` for easier understanding of the query structure. 
_Note that there might be a slight mismatch from the `.py` and the `.sql` files. The `.py` files are always the correct source-of-truth._

## Data
### TPC-H
The TPC-H must be generated through a series of steps. 
1. Download the [TPC-H Generator](https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-H&bm_vers=3.0.1&mode=CURRENT-ONLY). The generator must be located in `data/tpch/`.
2. Generate the data with the commant below. Add scale factor as suited (the majority of our tests are run with scale 0.5).
    ```sh
    python3 generateData.py <scale>
    ```
3. Convert the generated data to JSON by running the script below. 
    ```sh
    python3 convert_tpch_into_json.py
    ```
4. Create backup db file of the generated-and-converted data through the command below. Note that you will have to change the name of the db according to the scale factor (default is named `*_medium`).
    ```sh
    python3 populate_db.py
    ```

### Twitter
The Twitter dataset is for convenience stored as `data/twitter/twitter.json`.

## Tests
### Dataset Size - 4.2.2
The tests from Subsection 4.2.2 are used with TPC-H scale 0.1 and 1.0, and requires two runs through `perform_test.py` with `backup_path` set accordingly.

### Dataset Size - 4.2.3
The tests from Subsection 4.2.3 are used with several TPC-H scales - make sure you have followed the instruction from [TPC-H Setup](#tpc-h) for each required scale. Then, simply run (the ironically named) `verify_tpch_size_irrelevance.py`.

### Test Phase 1
Run `perform_load_test.py`. Make sure to
- Set `backup_path = f"./data/backup/{dataset}_medium"`
- Set `WORKLOAD_DISTRIBUTION = 'numerical'`
- Set `PHASE_3_ITERATION = 1`
- Set `DATASET = 'tpch'`
- Set `USE_WEIGHTS_IN_DECISION = FALSE`

### Test Phase 2
Run `perform_test_v2.py`. Make sure to
- Set `backup_path = f"./data/backup/{dataset}_medium"`

### Test Phase 3
The tests are, like in [Test Phase 1](#phase-1), run using `perform_load_test.py`. You can mix with the global variables to get the various test results from the phase.

### Materialization Cost
The tests are run using `perform_write_test.py`. 

### Comparing CTE and Single Access Method
The code generating queries using CTE-and-list-extract methods is, unfortunately, not to be found at present. Re-running these tests requires some copy-pasting from the file `perform_load_test.py` and `queries/query.py` from the commit [6a3a017](https://github.com/magnuis/duckdb-materialization/commit/6a3a017b763b81b8e2f4b85a80e8c2a5de65a4e7).

## Results
The results are to be found under their highly descriptive folder names nested under `results/`. 

## Visualizations
The `visualizations/` folder is a mess, and only meant for internal investigation of results. Please disregard the folder!


