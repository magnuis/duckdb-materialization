import testing.tpch.setup as tpch_setup
import pandas as pd
import duckdb
import argparse

DATASETS = {
    "tpch": {
        "queries": tpch_setup.QUERIES,
        "column_map": tpch_setup.COLUMN_MAP,
    }
}


def analyze_queries(data_set: str) -> dict:
    """
    Analyze the query frequency of provided dataset

    Returns
    -------
    dict
        The frequency per term
    """

    parser = argparse.ArgumentParser(
        description="Run performance tests on different datasets.")
    parser.add_argument("dataset", nargs="?", default="tpch", choices=["tpch"],
                        help="The dataset to run tests on (tpch, yelp, twitter, or all)")
    args = parser.parse_args()

    # datasets_to_test = DATASETS.keys() if args.dataset == "all" else [
    #     args.dataset]

    datasets_to_test = ['tpch']

    for dataset in datasets_to_test:

        config = DATASETS[dataset]

        queries: list = config["queries"]
        column_map: dict = config["column_map"]

        dataset_freq = {query: 0 for query in column_map}

        print(dataset_freq)

        for query in queries:

            with open(f"./queries/{dataset}/{query}.sql", 'r') as f:
                query = f.read()

            for column in dataset_freq:
                if column in query:
                    dataset_freq[column] += 1

        dataset_freq_list = [{"Column": key, "Absolute frequency": value,
                              "Relative frequency": round(value/len(queries), 2)} for key, value in dataset_freq.items()]
        # Create a DataFrame from the frequency dictionary
        df = pd.DataFrame(dataset_freq_list, columns=[
                          "Column", "Absolute frequency", "Relative frequency"])

        df.sort_values(by=["Absolute frequency"],
                       ascending=False, inplace=True)

        df.to_csv(
            f'data/{data_set}/query_frequency_{data_set}.csv', index=False)

        # Optionally print the DataFrame for this dataset
        print(f"DataFrame for dataset '{dataset}':")
        print(df)


if __name__ == "__main__":
    analyze_queries(data_set='tpch')
