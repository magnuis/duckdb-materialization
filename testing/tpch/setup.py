QUERIES = [
    'q1',
    'q2',
]

TESTS = {
    "no_materialization": {
        "materialization": [],
    },
    "full_materialization": {
        "materialization": [
            "l_returnflag",
            "l_linestatus",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_shipdate",
            "l_tax"
        ],
    },
    "partly_materialization": {
        "materialization": ["l_returnflag", "l_linestatus"],
    }
}


COLUMN_MAP = {
    ########################### C ###########################
    "c_custkey": {
        'query': "CAST(raw_json->>'c_custkey' AS INT)",
        'type': 'INT'
    },
    "c_nationkey": {
        'query': "CAST(raw_json->>'c_nationkey' AS INT)",
        'type': 'INT'
    },
    "c_mktsegment": {
        'query': "raw_json->>'c_mktsegment'",
        'type': 'VARCHAR'
    },
    ########################### L ###########################
    "l_orderkey": {
        'query': "CAST(raw_json->>'l_orderkey' AS INT)",
        'type': 'INT'
    },
    "l_suppkey": {
        'query': "CAST(raw_json->>'l_suppkey' AS INT)",
        'type': 'INT'
    },
    "l_partkey": {
        'query': "CAST(raw_json->>'l_partkey' AS INT)",
        'type': 'INT'
    },
    "l_returnflag": {
        'query': "CAST(raw_json->>'l_returnflag' AS CHAR(1))",
        'type': 'CHAR(1)'
    },
    'l_linestatus': {
        "query": "CAST(raw_json->>'l_linestatus' AS CHAR(1))",
        "type": "CHAR(1)"
    },
    "l_quantity": {
        'query': "CAST(raw_json->>'l_quantity' AS INT)",
        'type': 'INT'
    },
    "l_tax": {
        'query': "CAST(raw_json->>'l_tax' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "l_extendedprice": {
        'query': "CAST(raw_json->>'l_extendedprice' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "l_discount": {
        'query': "CAST(raw_json->>'l_discount' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "l_commitdate": {
        'query': "CAST(raw_json->>'l_commitdate' AS DATE)",
        'type': 'DATE'
    },
    "l_receiptdate": {
        'query': "CAST(raw_json->>'l_receiptdate' AS DATE)",
        'type': 'DATE'
    },
    "l_shipdate": {
        'query': "CAST(raw_json->>'l_shipdate' AS DATE)",
        'type': 'DATE'
    },
    ########################### N ###########################
    "n_nationkey": {
        'query': "CAST(raw_json->>'n_nationkey' AS INT)",
        'type': 'INT'
    },
    "n_regionkey": {
        'query': "CAST(raw_json->>'n_regionkey' AS INT)",
        'type': 'INT'
    },
    "n_name": {
        'query': "raw_json->>'n_name'",
        'type': 'VARCHAR'
    },
    ########################### O ###########################
    "o_orderdate": {
        'query': "CAST(raw_json->>'o_orderdate' AS DATE)",
        'type': 'DATE'
    },
    "o_shippriority": {
        'query': "CAST(raw_json->>'o_shippriority' AS INT)",
        'type': 'INT'
    },
    "o_custkey": {
        'query': "CAST(raw_json->>'o_custkey' AS INT)",
        'type': 'INT'
    },
    "o_orderkey": {
        'query': "CAST(raw_json->>'o_orderkey' AS INT)",
        'type': 'INT'
    },
    "o_orderpriority": {
        'query': "raw_json->>'o_orderpriority'",
        'type': 'VARCHAR'
    },

    ########################### P ###########################
    "p_type": {
        'query': "raw_json->>'p_type'",
        'type': 'VARCHAR'
    },
    "p_partkey": {
        'query': "CAST(raw_json->>'p_partkey' AS INT)",
        'type': 'INT'
    },
    "p_size": {
        'query': "CAST(raw_json->>'p_size' AS INT)",
        'type': 'INT'
    },
    "p_mfgr": {
        'query': "raw_json->>'p_mfgr'",
        'type': 'VARCHAR'
    },
    ########################## PS ###########################
    "ps_suppkey": {
        'query': "CAST(raw_json->>'ps_suppkey' AS INT)",
        'type': 'INT'
    },
    "ps_partkey": {
        'query': "CAST(raw_json->>'ps_partkey' AS INT)",
        'type': 'INT'
    },
    "ps_supplycost": {
        'query': "CAST(raw_json->>'ps_supplycost' AS INT)",
        'type': 'INT'
    },
    ########################### R ###########################
    "r_regionkey": {
        'query': "CAST(raw_json->>'r_regionkey' AS INT)",
        'type': 'INT'
    },
    "r_name": {
        'query': "raw_json->>'r_name'",
        'type': 'VARCHAR'
    },
    ########################### S ###########################
    "s_acctbal": {
        'query': "CAST(raw_json->>'s_acctbal' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "s_suppkey": {
        'query': "CAST(raw_json->>'s_suppkey' AS INT)",
        'type': "INT"
    },
    "s_nationkey": {
        'query': "CAST(raw_json->>'s_nationkey' AS INT)",
        'type': "INT"
    },
    "s_type": {
        'query': "raw_json->>'s_type'",
        'type': 'VARCHAR'
    },
    "s_name": {
        'query': "raw_json->>'s_name'",
        'type': 'VARCHAR'
    },
    "s_address": {
        'query': "raw_json->>'s_address'",
        'type': 'VARCHAR'
    },
    "s_phone": {
        'query': "raw_json->>'s_phone'",
        'type': 'VARCHAR'
    },
    "s_comment": {
        'query': "raw_json->>'s_comment'",
        'type': 'VARCHAR'
    }
}
