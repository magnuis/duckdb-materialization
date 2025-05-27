import json
import time
import os
import shutil
from collections import defaultdict
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CONFIG = {
    "tpch": {
        "json_paths": ['./data/tpch/tpch_json.json'],
    },
    "yelp": {
        "json_paths": [
            './data/yelp/yelp_academic_dataset_business.json',
            './data/yelp/yelp_academic_dataset_checkin.json',
            './data/yelp/yelp_academic_dataset_review.json',
            './data/yelp/yelp_academic_dataset_tip.json',
            './data/yelp/yelp_academic_dataset_user.json',
        ],
    }
}

DATA_PATH = './data'

CLEAN_UP_FILES = set()


def main(dataset: str):
    start_time = time.perf_counter()

    config = CONFIG[dataset]

    _prepare_dirs(dataset=dataset)

    distribution = defaultdict()

    # for file_path in config['json_paths']:

    _clean_up()

    print(
        f"Total time for populating db: {round(time.perf_counter() - start_time, 2)}s")


def _analyze_file(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            json_obj = json.loads(line)


def _clean_up():
    for file in list(CLEAN_UP_FILES):
        try:
            os.remove(file)
            print(f"Deleted file {file}")
        except FileNotFoundError:
            print(f"No file at path {file}")


def _prepare_dirs(dataset: str):
    if not os.path.isdir(DATA_PATH):
        os.mkdir(DATA_PATH)


if __name__ == "__main__":
    main(dataset='yelp')
