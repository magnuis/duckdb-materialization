QUERIES = {
    "q1": ["l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_shipdate" ,"l_tax"],
    "q2": [],
}

TESTS = {
    "no_materialization": {
        "materialization": [],
    },
    "full_materialization": {
        "materialization": ["l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_shipdate", "l_tax"],
    },
    "partly_materialization": {
        "materialization": ["l_returnflag", "l_linestatus"],
    }
}


COLUMN_MAP = {
    'l_returnflag': {
        "query": "CAST(raw_json->>'l_returnflag' AS CHAR(1))",
        "type": "CHAR(1)"
        },
    'l_linestatus': {
        "query": "CAST(raw_json->>'l_linestatus' AS CHAR(1))",
        "type": "CHAR(1)"
        },
    'l_quantity': {
        "query": "CAST(raw_json->>'l_quantity' AS INT)",
        "type": "INT"
        },
    'l_extendedprice': {
        "query": "CAST(raw_json->>'l_extendedprice' AS DECIMAL(12,2))",
        "type": "DECIMAL(12,2)"
        },
    'l_discount': {
        "query": "CAST(raw_json->>'l_discount' AS DECIMAL(12,2))",
        "type": "DECIMAL(12,2)"
        },
    'l_shipdate': {
        "query": "CAST(raw_json->>'l_shipdate' AS DATE)",
        "type": "DATE"
        },
    'l_tax': {
        "query": "CAST(raw_json->>'l_tax' AS DECIMAL(12,2))",
        "type": "DECIMAL(12,2)"
        },
}