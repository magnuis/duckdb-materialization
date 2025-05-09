from queries.query import Query


class Q8(Query):
    """
    TPC-H Query 8
    """

    def __init__(self):
        pass

#     def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
#         """
#         Get the formatted TPC-H query 8, adjusted to current db materializaiton

#         Returns
#         -------
#         str
#         """

#         dts = self._get_field_accesses(fields=fields)

#         return f"""
# SELECT
#     o_year,
#     SUM(CASE
#             WHEN nation = 'BRAZIL' THEN volume
#             ELSE 0
#         END) / SUM(volume) AS mkt_share
# FROM
#     (
#         SELECT
#             EXTRACT(YEAR FROM {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])}) AS o_year,
#             {self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])}) AS volume,
#             {self._json(tbl='n2', col='n_name', dt=dts['n_name'])} AS nation
#         FROM
#             test_table p,
#             test_table s,
#             test_table l,
#             test_table o,
#             test_table c,
#             test_table n1,
#             test_table n2,
#             test_table r
#         WHERE
#             {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
#             AND {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])}
#             AND {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} = {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])}
#             AND {self._json(tbl='o', col='o_custkey', dt=dts['o_custkey'])} = {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])}
#             AND {self._json(tbl='r', col='r_regionkey', dt=dts['r_regionkey'])} = {self._json(tbl='n1', col='n_regionkey', dt=dts['n_regionkey'])}
#             AND {self._json(tbl='c', col='c_nationkey', dt=dts['c_nationkey'])} = {self._json(tbl='n1', col='n_nationkey', dt=dts['n_nationkey'])}
#             AND {self._json(tbl='r', col='r_name', dt=dts['r_name'])} = 'AMERICA'
#             AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n2', col='n_nationkey', dt=dts['n_nationkey'])}
#             AND {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
#             AND {self._json(tbl='p', col='p_type', dt=dts['p_type'])} = 'ECONOMY ANODIZED STEEL'
#     ) AS all_nations
# GROUP BY
#     o_year
# ORDER BY
#     o_year;
#     """

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 8, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
        o_year,
        SUM(case
                WHEN nation = 'BRAZIL' THEN volume
                ELSE 0
        END) / SUM(volume) AS mkt_share
FROM
        (
                SELECT
                        extract(year FROM {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])}) AS o_year,
                        p_l_joined.l_extendedprice * (1  - p_l_joined.l_discount) AS volume,
                        {self._json(tbl='n2', col='n_name', dt=dts['n_name'])} AS nation
                FROM
                        (
                                SELECT
                                        {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} AS p_partkey,
                                        {self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} AS l_extendedprice,
                                        {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])} AS l_discount,
                                        {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} AS l_orderkey,
                                        {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])} AS l_partkey,
                                        {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])} AS l_suppkey
                                FROM 
                                        test_table p,
                                        test_table l
                                WHERE
                                        {self._json(tbl='p', col='p_type', dt=dts['p_type'])} = 'ECONOMY ANODIZED STEEL'
                                        AND {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}

                        ) AS p_l_joined,
                        (
                                SELECT
                                        {self._json(tbl='n1', col='n_nationkey', dt=dts['n_nationkey'])} AS n_nationkey
                                FROM
                                        test_table r,
                                        test_table n1
                                WHERE
                                        {self._json(tbl='r', col='r_name', dt=dts['r_name'])} = 'AMERICA'
                                        AND {self._json(tbl='r', col='r_regionkey', dt=dts['r_regionkey'])} = {self._json(tbl='n1', col='n_regionkey', dt=dts['n_regionkey'])}
                        ) AS r_n1_joined,

                        test_table o,
                        test_table c,
                        test_table s,
                        test_table n2
                WHERE

                        {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = p_l_joined.l_suppkey
                        AND p_l_joined.l_orderkey = {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])}
                        AND {self._json(tbl='o', col='o_custkey', dt=dts['o_custkey'])} = {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])}
                        AND {self._json(tbl='c', col='c_nationkey', dt=dts['c_nationkey'])} = r_n1_joined.n_nationkey
                        AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n2', col='n_nationkey', dt=dts['n_nationkey'])}
                        AND {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        ) AS all_nations
GROUP BY
        o_year
ORDER BY
        o_year;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 7

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """
        Get the underlying column names used in the query along with their position 
        in the query (e.g., SELECT, WHERE, GROUP BY, ORDER BY clauses).

        Returns
        -------
        dict
            A dictionary with the following keys:
            - 'select': list of underlying column names used in the SELECT clause.
            - 'where': list of underlying column names used in the WHERE clause that are not joins.
            - 'group_by': list of underlying column names used in the GROUP BY clause.
            - 'order_by': list of underlying column names used in the ORDER BY clause.
            - 'join': list of underlying column names used in a join operation (including WHERE)
        """
        return {
            'select': [
                "o_orderdate",
                "n_name",
                "p_partkey",
                "l_extendedprice",
                "l_discount",
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "n_nationkey"
            ],
            'where': [
                "p_type"
                "r_name",
                "o_orderdate",
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {
                "p_partkey": ["l_partkey"],
                "l_partkey": ["p_partkey"],
                "r_regionkey": ["n_regionkey"],
                "n_regionkey": ["r_regionkey"],
                "s_suppkey": [None],
                "o_orderkey": [None],
                "o_custkey": ["c_custkey"],
                "c_custkey": ["o_custkey"],
                "c_nationkey": [None],
                "s_nationkey": ["n_nationkey"],
                "n_nationkey": ["s_nationkey"]
            }
        }
