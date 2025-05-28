from queries.query import Query

from queries.tpch.q1 import Q1
from queries.tpch.q2 import Q2
from queries.tpch.q3 import Q3
from queries.tpch.q4 import Q4
from queries.tpch.q5 import Q5
from queries.tpch.q6 import Q6
from queries.tpch.q7 import Q7
from queries.tpch.q8 import Q8
from queries.tpch.q9 import Q9
from queries.tpch.q10 import Q10
from queries.tpch.q11 import Q11
from queries.tpch.q12 import Q12
from queries.tpch.q13 import Q13
from queries.tpch.q14 import Q14
from queries.tpch.q15 import Q15
from queries.tpch.q16 import Q16
from queries.tpch.q17 import Q17
from queries.tpch.q18 import Q18
from queries.tpch.q19 import Q19
from queries.tpch.q20 import Q20
from queries.tpch.q21 import Q21
from queries.tpch.q22 import Q22

QUERIES: dict[str, Query] = {
    'q1': Q1(),
    'q2': Q2(),
    'q3': Q3(),
    'q4': Q4(),
    # 'q5': Q5(),
    'q6': Q6(),
    'q7': Q7(),
    'q8': Q8(),
    'q9': Q9(),
    'q10': Q10(),
    'q11': Q11(),
    'q12': Q12(),
    'q13': Q13(),
    'q14': Q14(),
    'q15': Q15(),
    'q16': Q16(),
    'q17': Q17(),
    'q18': Q18(),
    'q19': Q19(),
    'q20': Q20(),
    'q21': Q21(),
    'q22': Q22(),
    # 'q23',
    # 'q24',
    # 'q25'
}

LOAD_BASED_MATERIALIZATION = [
    {
        "no_frequent_queries": 0,
        "no_queries": 0,
        "load": []
    }
]

STANDARD_SETUPS = {
    # "q13_05_1_01-05": {
    #     "materialization": ["c_custkey", "o_comment"],
    # },
    # "q13_075_0_01-05": {
    #     "materialization": ["c_custkey", "o_custkey", "o_orderkey"],
    # }
    # "q9_05_0_05-2-4": {
    #     "materialization": ['ps_supplycost', 'l_partkey', 's_nationkey', 'l_orderkey', 'o_orderdate', 'p_name', 'o_orderkey', 'ps_suppkey'],
    # },
    # "q9_05_1_05-2-4": {
    #     "materialization": ['o_orderdate', 'p_name', 'n_nationkey', 's_suppkey', 'l_extendedprice', 'p_partkey', 'ps_supplycost', 'l_partkey'],
    # },
    # "q18_25_1_05-4": {
    #     "materialization": ['c_name', 'l_orderkey'],
    # },
    # "q18_25_0_05-4": {
    #     "materialization": ['o_orderkey', 'o_totalprice'],
    # }
    # "q5_l1_m10": {
    #     "materialization": ['l_extendedprice', 'l_discount', 'o_orderkey', 'l_orderkey', 'o_orderdate', 'c_custkey', 'o_custkey', 's_suppkey', 'n_name', 'n_nationkey']
    # },
    # "q5_l1_m11": {
    #     "materialization": ['l_extendedprice', 'l_discount', 'o_orderkey', 'l_orderkey', 'o_orderdate', 'c_custkey', 'o_custkey', 's_suppkey', 'n_name', 'n_nationkey', 'l_suppkey']
    # },
    # "q5_l3_m11": {
    #     "materialization": ['o_orderkey', 'l_orderkey', 'l_extendedprice', 'o_custkey', 'c_custkey',
    #                         'l_discount', 'o_orderdate', 's_suppkey', 'n_name', 'n_nationkey', 's_nationkey']
    # },
    # "q5_l3_m12": {
    #     "materialization": ['o_orderkey', 'l_orderkey', 'l_extendedprice', 'o_custkey', 'c_custkey',
    #                         'l_discount', 'o_orderdate', 's_suppkey', 'n_name', 'n_nationkey', 's_nationkey', 'l_suppkey']
    # },
    # "q5_l7_m8": {
    #     "materialization": ['s_suppkey', 'n_nationkey', 'n_name', 's_nationkey', 'o_orderkey', 'l_orderkey', 'l_suppkey', 'c_custkey']
    # },
    # "q5_l7_m9": {
    #     "materialization": ['s_suppkey', 'n_nationkey', 'n_name', 's_nationkey', 'o_orderkey', 'l_orderkey', 'l_suppkey', 'c_custkey', 'o_custkey']
    # },
    # "q5_l8_m10": {
    #     "materialization": ['o_orderkey', 'l_orderkey', 'c_custkey', 'o_custkey', 'l_extendedprice', 'l_discount', 's_suppkey', 'n_nationkey', 'n_name', 'o_orderdate']
    # },
    # "q5_l8_m11": {
    #     "materialization": ['o_orderkey', 'l_orderkey', 'c_custkey', 'o_custkey', 'l_extendedprice', 'l_discount', 's_suppkey', 'n_nationkey', 'n_name', 'o_orderdate', 'l_suppkey']
    # }
    # "q3_l2_m10": {
    #     "materialization": ['o_orderkey', 's_suppkey', 'n_nationkey', 'n_name', 's_nationkey', 'l_orderkey', 'c_custkey', 'o_custkey', 's_name', 's_acctbal']
    # },
    "q3_l2_m11": {
        "materialization": ['o_orderkey', 's_suppkey', 'n_nationkey', 'n_name', 's_nationkey', 'l_orderkey', 'c_custkey', 'o_custkey', 's_name', 's_acctbal', 'l_extendedprice']
    },
    # "test": {
    #     "materialization": ["s_suppkey"],
    # },
    # "q4m400l9_1field": {
    #     "materialization": ["s_suppkey"],
    # },
    # "q4m400l9_2field": {
    #     "materialization": ['l_shipdate', 's_suppkey'],
    # },
    # "q4m400l9_3field": {
    #     "materialization": ['l_suppkey', 'l_shipdate', 's_suppkey'],
    # },
    # "q4m400l9_4field": {
    #     "materialization": ['l_suppkey', 'l_extendedprice', 'l_shipdate', 's_suppkey'],
    # },
    # "q4m400l9_5field": {
    #     "materialization": ['l_suppkey', 'l_extendedprice', 'l_discount', 'l_shipdate', 's_suppkey'],
    # },
    # "q4m400l9_6field": {
    #     "materialization": ['l_suppkey', 'l_extendedprice', 'l_discount', 'l_shipdate', 's_suppkey', 's_name'],
    # },
    # "q4m400l9_7field": {
    #     "materialization": ['l_suppkey', 'l_extendedprice', 'l_discount', 'l_shipdate', 'n_nationkey', 's_suppkey', 's_name'],
    # },
    # "q4m400l9_8field": {
    #     "materialization": ['l_suppkey', 'l_extendedprice', 'l_discount', 'l_shipdate', 'n_nationkey', 'n_name', 's_suppkey', 's_name'],
    # },
    # "q4m400l9_9field": {
    #     "materialization": ['l_suppkey', 'l_extendedprice', 'l_discount', 'l_shipdate', 'n_nationkey', 'n_name', 's_suppkey', 's_name', 's_address'],
    # },
    # "q4m400l9_10field": {
    #     "materialization": ['l_suppkey', 'l_extendedprice', 'l_discount', 'l_shipdate', 'n_nationkey', 'n_name', 's_suppkey', 's_name', 's_address', 's_nationkey'],
    # },
    # "q4m400l9_11field": {
    #     "materialization": ['l_suppkey', 'l_extendedprice', 'l_discount', 'l_shipdate', 'n_nationkey', 'n_name', 'p_partkey', 's_suppkey', 's_name', 's_address', 's_nationkey'],
    # },
    # "q4m400l9_12field": {
    #     "materialization": ['l_suppkey', 'l_extendedprice', 'l_discount', 'l_shipdate', 'n_nationkey', 'n_name', 'o_orderkey', 'p_partkey', 's_suppkey', 's_name', 's_address', 's_nationkey'],
    # },
    # "q4m400l9_13field": {
    #     "materialization": ['l_orderkey', 'l_suppkey', 'l_extendedprice', 'l_discount', 'l_shipdate', 'n_nationkey', 'n_name', 'o_orderkey', 'p_partkey', 's_suppkey', 's_name', 's_address', 's_nationkey'],
    # },





    # "q2_l0": {
    #     "materialization": ["p_mfgr", "n_name", "p_partkey", "ps_partkey"]
    # },
    "no_materialization": {
        "materialization": [],
    },
    "full_materialization": {
        "materialization": None,
    },
    # "load_based_materialization": {
    #     "materialization": [
    #         "l_extendedprice",
    #         "o_orderkey",
    #         "l_discount",
    #         "s_suppkey",
    #         "l_orderkey",
    #         "n_name",
    #         "n_nationkey",
    #         "c_custkey",
    #         "p_partkey",
    #         "o_custkey",
    #         "s_nationkey",
    #         "l_shipdate"
    #     ]
    # },
    # "schema_based_materialization": {
    #     "materialization": [
    #         "l_orderkey",
    #         "l_suppkey",
    #         "l_partkey",
    #         "l_returnflag",
    #         'l_linestatus',
    #         "l_quantity",
    #         "l_tax",
    #         "l_extendedprice",
    #         "l_discount",
    #         "l_commitdate",
    #         "l_receiptdate",
    #         "l_shipdate",
    #         "l_shipmode",
    #         "l_shipinstruct",
    #         "l_comment",
    #         "l_linenumber"
    #     ],
    # },
}


COLUMN_MAP = {
    ########################### C ###########################
    "c_custkey": {
        'access': "CAST(raw_json->>'c_custkey' AS INT)",
        'type': 'INT'
    },
    "c_nationkey": {
        'access': "CAST(raw_json->>'c_nationkey' AS INT)",
        'type': 'INT'
    },
    "c_mktsegment": {
        'access': "raw_json->>'c_mktsegment'",
        'type': 'VARCHAR'
    },
    "c_name": {
        'access': "raw_json->>'c_name'",
        'type': 'VARCHAR'
    },
    "c_phone": {
        'access': "raw_json->>'c_phone'",
        'type': 'VARCHAR'
    },
    "c_address": {
        'access': "raw_json->>'c_address'",
        'type': 'VARCHAR'
    },
    "c_comment": {
        'access': "raw_json->>'c_comment'",
        'type': 'VARCHAR'
    },
    "c_acctbal": {
        'access': "CAST(raw_json->>'c_acctbal' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    ########################### L ###########################
    "l_orderkey": {
        'access': "CAST(raw_json->>'l_orderkey' AS INT)",
        'type': 'INT'
    },
    "l_partkey": {
        'access': "CAST(raw_json->>'l_partkey' AS INT)",
        'type': 'INT'
    },
    "l_suppkey": {
        'access': "CAST(raw_json->>'l_suppkey' AS INT)",
        'type': 'INT'
    },
    'l_linenumber': {
        "access": "CAST(raw_json->>'l_linenumber' AS INT)",
        "type": "INT"
    },
    "l_quantity": {
        'access': "CAST(raw_json->>'l_quantity' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    "l_extendedprice": {
        'access': "CAST(raw_json->>'l_extendedprice' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "l_discount": {
        'access': "CAST(raw_json->>'l_discount' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "l_tax": {
        'access': "CAST(raw_json->>'l_tax' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "l_returnflag": {
        'access': "CAST(raw_json->>'l_returnflag' AS CHAR(1))",
        'type': 'CHAR(1)'
    },
    'l_linestatus': {
        "access": "CAST(raw_json->>'l_linestatus' AS CHAR(1))",
        "type": "CHAR(1)"
    },
    "l_shipdate": {
        'access': "CAST(raw_json->>'l_shipdate' AS DATE)",
        'type': 'DATE'
    },
    "l_commitdate": {
        'access': "CAST(raw_json->>'l_commitdate' AS DATE)",
        'type': 'DATE'
    },
    "l_receiptdate": {
        'access': "CAST(raw_json->>'l_receiptdate' AS DATE)",
        'type': 'DATE'
    },
    "l_shipinstruct": {
        'access': "raw_json->>'l_shipinstruct'",
        'type': 'VARCHAR'
    },
    "l_shipmode": {
        'access': "raw_json->>'l_shipmode'",
        'type': 'VARCHAR'
    },
    "l_comment": {
        'access': "raw_json->>'l_comment'",
        'type': 'VARCHAR'
    },
    ########################### N ###########################
    "n_nationkey": {
        'access': "CAST(raw_json->>'n_nationkey' AS INT)",
        'type': 'INT'
    },
    "n_regionkey": {
        'access': "CAST(raw_json->>'n_regionkey' AS INT)",
        'type': 'INT'
    },
    "n_name": {
        'access': "raw_json->>'n_name'",
        'type': 'VARCHAR'
    },
    ########################### O ###########################
    "o_orderdate": {
        'access': "CAST(raw_json->>'o_orderdate' AS DATE)",
        'type': 'DATE'
    },
    "o_totalprice": {
        'access': "CAST(raw_json->>'o_totalprice' AS DECIMAL(12,2))",
        'type': 'DECIMAL(12,2)'
    },
    "o_shippriority": {
        'access': "CAST(raw_json->>'o_shippriority' AS INT)",
        'type': 'INT'
    },
    "o_custkey": {
        'access': "CAST(raw_json->>'o_custkey' AS INT)",
        'type': 'INT'
    },
    "o_orderkey": {
        'access': "CAST(raw_json->>'o_orderkey' AS INT)",
        'type': 'INT'
    },
    "o_orderpriority": {
        'access': "raw_json->>'o_orderpriority'",
        'type': 'VARCHAR'
    },
    "o_comment": {
        'access': "raw_json->>'o_comment'",
        'type': 'VARCHAR'
    },
    'o_orderstatus': {
        "access": "CAST(raw_json->>'o_orderstatus' AS CHAR(1))",
        "type": "CHAR(1)"
    },
    ########################### P ###########################
    "p_type": {
        'access': "raw_json->>'p_type'",
        'type': 'VARCHAR'
    },
    "p_name": {
        'access': "raw_json->>'p_name'",
        'type': 'VARCHAR'
    },
    "p_partkey": {
        'access': "CAST(raw_json->>'p_partkey' AS INT)",
        'type': 'INT'
    },
    "p_size": {
        'access': "CAST(raw_json->>'p_size' AS INT)",
        'type': 'INT'
    },
    "p_mfgr": {
        'access': "raw_json->>'p_mfgr'",
        'type': 'VARCHAR'
    },
    "p_brand": {
        'access': "raw_json->>'p_brand'",
        'type': 'VARCHAR'
    },
    "p_container": {
        'access': "raw_json->>'p_container'",
        'type': 'VARCHAR'
    },
    ########################## PS ###########################
    "ps_partkey": {
        'access': "CAST(raw_json->>'ps_partkey' AS INT)",
        'type': 'INT'
    },
    "ps_suppkey": {
        'access': "CAST(raw_json->>'ps_suppkey' AS INT)",
        'type': 'INT'
    },
    "ps_availqty": {
        'access': "CAST(raw_json->>'ps_availqty' AS INT)",
        'type': 'INT'
    },
    "ps_supplycost": {
        'access': "CAST(raw_json->>'ps_supplycost' AS INT)",
        'type': 'INT'
    },
    "ps_comment": {
        'access': "raw_json->>'ps_comment'",
        'type': 'VARCHAR'
    },
    ########################### R ###########################
    "r_regionkey": {
        'access': "CAST(raw_json->>'r_regionkey' AS INT)",
        'type': 'INT'
    },
    "r_comment": {
        'access': "raw_json->>'r_comment'",
        'type': 'VARCHAR'
    },
    "r_name": {
        'access': "raw_json->>'r_name'",
        'type': 'VARCHAR'
    },
    ########################### S ###########################
    "s_suppkey": {
        'access': "CAST(raw_json->>'s_suppkey' AS INT)",
        'type': "INT"
    },
    "s_name": {
        'access': "raw_json->>'s_name'",
        'type': 'VARCHAR'
    },
    "s_address": {
        'access': "raw_json->>'s_address'",
        'type': 'VARCHAR'
    },
    "s_nationkey": {
        'access': "CAST(raw_json->>'s_nationkey' AS INT)",
        'type': "INT"
    },
    "s_phone": {
        'access': "raw_json->>'s_phone'",
        'type': 'VARCHAR'
    },
    "s_acctbal": {
        'access': "CAST(raw_json->>'s_acctbal' AS DECIMAL(12,2))",
        'type': "DECIMAL(12,2)"
    },
    "s_comment": {
        'access': "raw_json->>'s_comment'",
        'type': 'VARCHAR'
    }
}
