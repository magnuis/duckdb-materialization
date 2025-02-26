QUERIES = [
    'q1',
    # 'q2',
    # 'q3',
    # 'q4',
    # 'q5',
    # 'q6',
    # 'q7',
    # 'q8',
    # 'q9',
    # 'q10',
    # 'q11',
    # 'q12',
    # 'q13',
    # 'q14',
    # 'q15',
    # 'q16',
    # 'q17',
    # 'q18',
    # 'q19',
    # 'q20',
    # 'q21',
    # 'q22',
    # 'q23',
]

TESTS = {
    "no_materialization": {
        "materialization": [],
    },
    "full_materialization": {
        "materialization": None,
    },

    "load_based_materialization": {
        "materialization": [
            "l_extendedprice",
            "o_orderkey",
            "l_discount",
            "s_suppkey",
            "l_orderkey",
            "n_name",
            "n_nationkey",
            "c_custkey",
            "p_partkey",
            "o_custkey",
            "s_nationkey",
            "l_shipdate"
        ]
    },
    "schema_based_materialization": {
        "materialization": [
            "l_orderkey",
            "l_suppkey",
            "l_partkey",
            "l_returnflag",
            'l_linestatus',
            "l_quantity",
            "l_tax",
            "l_extendedprice",
            "l_discount",
            "l_commitdate",
            "l_receiptdate",
            "l_shipdate",
            "l_shipmode",
            "l_shipinstruct",
            # "c_mktsegment",
            # "c_name",
            # "c_phone",
            # "c_address",
            # "c_comment",
            # "s_name",
            # "s_phone",
            # "s_address",
            # "s_comment",
            # "p_name",
            # "p_type",
            # "p_partkey",
            # # "p_size",
            # # "p_mfgr"
        ],
    },
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
    "c_name": {
        'query': "raw_json->>'c_name'",
        'type': 'VARCHAR'
    },
    "c_phone": {
        'query': "raw_json->>'c_phone'",
        'type': 'VARCHAR'
    },
    "c_address": {
        'query': "raw_json->>'c_address'",
        'type': 'VARCHAR'
    },
    "c_comment": {
        'query': "raw_json->>'c_comment'",
        'type': 'VARCHAR'
    },
    "c_acctbal": {
        'query': "CAST(raw_json->>'c_acctbal' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    ########################### L ###########################
    "l_orderkey": {
        'query': "CAST(raw_json->>'l_orderkey' AS INT)",
        'type': 'INT'
    },
    "l_partkey": {
        'query': "CAST(raw_json->>'l_partkey' AS INT)",
        'type': 'INT'
    },
    "l_suppkey": {
        'query': "CAST(raw_json->>'l_suppkey' AS INT)",
        'type': 'INT'
    },
    'l_linenumber': {
        "query": "CAST(raw_json->>'l_linenumber' AS INT)",
        "type": "INT"
    },
    "l_quantity": {
        'query': "CAST(raw_json->>'l_quantity' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    "l_extendedprice": {
        'query': "CAST(raw_json->>'l_extendedprice' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "l_discount": {
        'query': "CAST(raw_json->>'l_discount' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "l_tax": {
        'query': "CAST(raw_json->>'l_tax' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "l_returnflag": {
        'query': "CAST(raw_json->>'l_returnflag' AS CHAR(1))",
        'type': 'CHAR(1)'
    },
    'l_linestatus': {
        "query": "CAST(raw_json->>'l_linestatus' AS CHAR(1))",
        "type": "CHAR(1)"
    },
    "l_shipdate": {
        'query': "CAST(raw_json->>'l_shipdate' AS DATE)",
        'type': 'DATE'
    },
    "l_commitdate": {
        'query': "CAST(raw_json->>'l_commitdate' AS DATE)",
        'type': 'DATE'
    },
    "l_receiptdate": {
        'query': "CAST(raw_json->>'l_receiptdate' AS DATE)",
        'type': 'DATE'
    },
    "l_shipinstruct": {
        'query': "raw_json->>'l_shipinstruct'",
        'type': 'VARCHAR'
    },
    "l_shipmode": {
        'query': "raw_json->>'l_shipmode'",
        'type': 'VARCHAR'
    },
    "l_comment": {
        'query': "raw_json->>'l_comment'",
        'type': 'VARCHAR'
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
    "o_totalprice": {
        'query': "CAST(raw_json->>'o_totalprice' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
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
    "o_comment": {
        'query': "raw_json->>'o_comment'",
        'type': 'VARCHAR'
    },
    'o_orderstatus': {
        "query": "CAST(raw_json->>'o_orderstatus' AS CHAR(1))",
        "type": "CHAR(1)"
    },
    ########################### P ###########################
    "p_type": {
        'query': "raw_json->>'p_type'",
        'type': 'VARCHAR'
    },
    "p_name": {
        'query': "raw_json->>'p_name'",
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
    "p_brand": {
        'query': "raw_json->>'p_brand'",
        'type': 'VARCHAR'
    },
    "p_container": {
        'query': "raw_json->>'p_container'",
        'type': 'VARCHAR'
    },
    ########################## PS ###########################
    "ps_suppkey": {
        'query': "CAST(raw_json->>'ps_suppkey' AS INT)",
        'type': 'INT'
    },
    "ps_availqty": {
        'query': "CAST(raw_json->>'ps_availqty' AS INT)",
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
