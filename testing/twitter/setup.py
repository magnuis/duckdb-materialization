from queries.query import Query

from queries.tpch.q1 import Q1

QUERIES: dict[str, Query] = {
    'q1': Q1(),
}

COLUMN_MAP = {
    'lang': {
        'access': "TRY_CAST(raw_json->>'lang' AS VARCHAR)",
        "TYPE": 'VARCHAR'
    }
}
