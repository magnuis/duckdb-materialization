import os
import argparse
import testing.tpch.setup as tpch_setup
import pandas as pd

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

    datasets_to_test = [data_set]

    for dataset in datasets_to_test:
        previously_computed = os.path.exists(
            f'./results/{data_set}/query_frequency_{data_set}.csv'
        ) and os.path.exists(
            f'./results/{data_set}/field_distribution_{data_set}.csv'
        )
        if previously_computed:
            continue

        config = DATASETS[dataset]

        queries: list = config["queries"]
        column_map: dict = config["column_map"]

        dataset_freq = {query: 0 for query in column_map}
        field_distributions = []

        for query in queries:
            field_distribution = {"query": query}

            with open(f"./queries/{dataset}/{query}.sql", 'r') as f:
                query = f.read()

            for column in dataset_freq:
                if column in query:
                    dataset_freq[column] += 1
                    field_distribution[column] = 1
                else:
                    field_distribution[column] = 0

            field_distributions.append(field_distribution)

        dataset_freq_list = [{"Column": key, "Absolute frequency": value,
                              "Relative frequency": round(value/len(queries), 2)} for key, value in dataset_freq.items()]

        # Create a DataFrame from the frequency dictionary
        freq_df = pd.DataFrame(dataset_freq_list, columns=[
            "Column", "Absolute frequency", "Relative frequency"])

        freq_df.sort_values(by=["Absolute frequency"],
                            ascending=False, inplace=True)

        freq_df.to_csv(
            f'./results/{data_set}/query_frequency_{data_set}.csv', index=False)

        # Create a DataFrame from the distribution list
        dist_df = pd.DataFrame(field_distributions)

        dist_df.to_csv(
            f'./results/{data_set}/field_distribution_{data_set}.csv', index=False)

        # Optionally print the DataFrame for this dataset
        print(f"DataFrame for dataset '{dataset}':")
        print(freq_df)


if __name__ == "__main__":
    analyze_queries(data_set='tpch')
