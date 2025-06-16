# pylint: disable=W0611
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
    'q1': Q1(dataset='tpch'),
    'q2': Q2(dataset='tpch'),
    'q3': Q3(dataset='tpch'),
    'q4': Q4(dataset='tpch'),
    # 'q5': Q5(dataset='tpch'),
    'q6': Q6(dataset='tpch'),
    'q7': Q7(dataset='tpch'),
    'q8': Q8(dataset='tpch'),
    'q9': Q9(dataset='tpch'),
    'q10': Q10(dataset='tpch'),
    'q11': Q11(dataset='tpch'),
    'q12': Q12(dataset='tpch'),
    'q13': Q13(dataset='tpch'),
    'q14': Q14(dataset='tpch'),
    'q15': Q15(dataset='tpch'),
    'q16': Q16(dataset='tpch'),
    'q17': Q17(dataset='tpch'),
    'q18': Q18(dataset='tpch'),
    'q19': Q19(dataset='tpch'),
    'q20': Q20(dataset='tpch'),
    'q21': Q21(dataset='tpch'),
    'q22': Q22(dataset='tpch')
}


STANDARD_SETUPS = {
    "no_materialization": {
        "materialization": [],
    },
    "full_materialization": {
        "materialization": None,
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
            "l_comment",
            "l_linenumber"
        ],
    },
}


COLUMN_MAP = {
    ########################### C ###########################
    "c_custkey": {
        "access": "raw_json->>'c_custkey'",
        "type": "INT"
    },
    "c_nationkey": {
        "access": "raw_json->>'c_nationkey'",
        "type": "INT"
    },
    "c_mktsegment": {
        "access": "raw_json->>'c_mktsegment'",
        "type": "VARCHAR"
    },
    "c_name": {
        "access": "raw_json->>'c_name'",
        "type": "VARCHAR"
    },
    "c_phone": {
        "access": "raw_json->>'c_phone'",
        "type": "VARCHAR"
    },
    "c_address": {
        "access": "raw_json->>'c_address'",
        "type": "VARCHAR"
    },
    "c_comment": {
        "access": "raw_json->>'c_comment'",
        "type": "VARCHAR"
    },
    "c_acctbal": {
        "access": "raw_json->>'c_acctbal'",
        "type": "DECIMAL(12,2)"
    },
    ########################### L ###########################
    "l_orderkey": {
        "access": "raw_json->>'l_orderkey'",
        "type": "INT"
    },
    "l_partkey": {
        "access": "raw_json->>'l_partkey'",
        "type": "INT"
    },
    "l_suppkey": {
        "access": "raw_json->>'l_suppkey'",
        "type": "INT"
    },
    'l_linenumber': {
        "access": "raw_json->>'l_linenumber'",
        "type": "INT"
    },
    "l_quantity": {
        "access": "raw_json->>'l_quantity'",
        "type": "DECIMAL(12,2)"
    },
    "l_extendedprice": {
        "access": "raw_json->>'l_extendedprice'",
        "type": "DECIMAL(12,2)"
    },
    "l_discount": {
        "access": "raw_json->>'l_discount'",
        "type": "DECIMAL(12,2)"
    },
    "l_tax": {
        "access": "raw_json->>'l_tax'",
        "type": "DECIMAL(12,2)"
    },
    "l_returnflag": {
        "access": "raw_json->>'l_returnflag'",
        "type": "CHAR(1)"
    },
    'l_linestatus': {
        "access": "raw_json->>'l_linestatus'",
        "type": "CHAR(1)"
    },
    "l_shipdate": {
        "access": "raw_json->>'l_shipdate'",
        "type": "DATE"
    },
    "l_commitdate": {
        "access": "raw_json->>'l_commitdate'",
        "type": "DATE"
    },
    "l_receiptdate": {
        "access": "raw_json->>'l_receiptdate'",
        "type": "DATE"
    },
    "l_shipinstruct": {
        "access": "raw_json->>'l_shipinstruct'",
        "type": "VARCHAR"
    },
    "l_shipmode": {
        "access": "raw_json->>'l_shipmode'",
        "type": "VARCHAR"
    },
    "l_comment": {
        "access": "raw_json->>'l_comment'",
        "type": "VARCHAR"
    },
    ########################### N ###########################
    "n_nationkey": {
        "access": "raw_json->>'n_nationkey'",
        "type": "INT"
    },
    "n_regionkey": {
        "access": "raw_json->>'n_regionkey'",
        "type": "INT"
    },
    "n_name": {
        "access": "raw_json->>'n_name'",
        "type": "VARCHAR"
    },
    ########################### O ###########################
    "o_orderdate": {
        "access": "raw_json->>'o_orderdate'",
        "type": "DATE"
    },
    "o_totalprice": {
        "access": "raw_json->>'o_totalprice'",
        "type": "DECIMAL(12,2)"
    },
    "o_shippriority": {
        "access": "raw_json->>'o_shippriority'",
        "type": "INT"
    },
    "o_custkey": {
        "access": "raw_json->>'o_custkey'",
        "type": "INT"
    },
    "o_orderkey": {
        "access": "raw_json->>'o_orderkey'",
        "type": "INT"
    },
    "o_orderpriority": {
        "access": "raw_json->>'o_orderpriority'",
        "type": "VARCHAR"
    },
    "o_comment": {
        "access": "raw_json->>'o_comment'",
        "type": "VARCHAR"
    },
    'o_orderstatus': {
        "access": "raw_json->>'o_orderstatus'",
        "type": "CHAR(1)"
    },
    ########################### P ###########################
    "p_type": {
        "access": "raw_json->>'p_type'",
        "type": "VARCHAR"
    },
    "p_name": {
        "access": "raw_json->>'p_name'",
        "type": "VARCHAR"
    },
    "p_partkey": {
        "access": "raw_json->>'p_partkey'",
        "type": "INT"
    },
    "p_size": {
        "access": "raw_json->>'p_size'",
        "type": "INT"
    },
    "p_mfgr": {
        "access": "raw_json->>'p_mfgr'",
        "type": "VARCHAR"
    },
    "p_brand": {
        "access": "raw_json->>'p_brand'",
        "type": "VARCHAR"
    },
    "p_container": {
        "access": "raw_json->>'p_container'",
        "type": "VARCHAR"
    },
    ########################## PS ###########################
    "ps_partkey": {
        "access": "raw_json->>'ps_partkey'",
        "type": "INT"
    },
    "ps_suppkey": {
        "access": "raw_json->>'ps_suppkey'",
        "type": "INT"
    },
    "ps_availqty": {
        "access": "raw_json->>'ps_availqty'",
        "type": "INT"
    },
    "ps_supplycost": {
        "access": "raw_json->>'ps_supplycost'",
        "type": "INT"
    },
    "ps_comment": {
        "access": "raw_json->>'ps_comment'",
        "type": "VARCHAR"
    },
    ########################### R ###########################
    "r_regionkey": {
        "access": "raw_json->>'r_regionkey'",
        "type": "INT"
    },
    "r_comment": {
        "access": "raw_json->>'r_comment'",
        "type": "VARCHAR"
    },
    "r_name": {
        "access": "raw_json->>'r_name'",
        "type": "VARCHAR"
    },
    ########################### S ###########################
    "s_suppkey": {
        "access": "raw_json->>'s_suppkey'",
        "type": "INT"
    },
    "s_name": {
        "access": "raw_json->>'s_name'",
        "type": "VARCHAR"
    },
    "s_address": {
        "access": "raw_json->>'s_address'",
        "type": "VARCHAR"
    },
    "s_nationkey": {
        "access": "raw_json->>'s_nationkey'",
        "type": "INT"
    },
    "s_phone": {
        "access": "raw_json->>'s_phone'",
        "type": "VARCHAR"
    },
    "s_acctbal": {
        "access": "raw_json->>'s_acctbal'",
        "type": "DECIMAL(12,2)"
    },
    "s_comment": {
        "access": "raw_json->>'s_comment'",
        "type": "VARCHAR"
    }
}
