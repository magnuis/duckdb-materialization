import random
from queries.query import Query


QUERIES_IN_LOAD = 500
QUERY_PROPORTIONS = [4]
MAJORITY_PROPORTIONS = [0.80]
NO_LOADS = 10


def numerical_distribution(queries: dict[str, Query]):

    load_dicts = []

    all_queries = list(queries.keys())

    qm = [(q, int(m*QUERIES_IN_LOAD))
          for q in QUERY_PROPORTIONS for m in MAJORITY_PROPORTIONS]

    last_load_length = QUERIES_IN_LOAD

    for query_proportion, majority_proportion in qm:

        loads = []
        majority_queries = []

        for i in range(NO_LOADS):
            r = random.Random()
            r.seed(i)

            load_majority_queries = r.sample(
                population=all_queries, k=query_proportion, )
            minority_queries = [
                q for q in all_queries if q not in load_majority_queries]

            maj_load = [r.choice(load_majority_queries)
                        for _ in range(majority_proportion)]
            min_load = [r.choice(minority_queries)
                        for _ in range(QUERIES_IN_LOAD - majority_proportion)]

            load = maj_load + min_load

            r.shuffle(load)

            loads.append(load)
            majority_queries.append(
                sorted(load_majority_queries, key=lambda x: int(x[1:])))

            assert last_load_length == len(load)
            last_load_length = len(load)

        load_dicts.append({
            "loads": loads,
            "query_proportion": query_proportion,
            "majority_proportion": majority_proportion,
            "majority_queries": majority_queries
        })

    return load_dicts


def random_distribution(queries: dict[str, Query]):

    load_dicts = []

    all_queries = list(queries.keys())

    qm = [(q, int(m*QUERIES_IN_LOAD))
          for q in QUERY_PROPORTIONS for m in MAJORITY_PROPORTIONS]

    last_load_length = QUERIES_IN_LOAD

    for query_proportion, majority_proportion in qm:

        loads = []
        majority_queries = []

        for i in range(NO_LOADS):
            r = random.Random()
            r.seed(i)

            load = [r.choice(all_queries) for _ in range(500)]

            r.shuffle(load)

            loads.append(load)
            majority_queries.append(None)

            assert last_load_length == len(load)
            last_load_length = len(load)

        load_dicts.append({
            "loads": loads,
            "query_proportion": query_proportion,
            "majority_proportion": majority_proportion,
            "majority_queries": majority_queries
        })

    return load_dicts
