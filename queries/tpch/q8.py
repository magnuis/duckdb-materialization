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

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "p": ["p_partkey", "p_type"],
            "l": ["l_extendedprice", "l_discount", "l_orderkey", "l_partkey", "l_suppkey"],
            "r": ["r_name", "r_regionkey"],
            "n1": ["n_nationkey", "n_regionkey", "n_name"],
            "n2": ["n_nationkey", "n_name"],
            "o": ["o_custkey", "o_orderdate", "o_orderkey"],
            "c": ["c_nationkey", "c_custkey"],
            "s": ["s_nationkey", "s_suppkey"]
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 8, adjusted to current db materializaiton

        Returns
        -------
        str
        """

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
                        extract(year FROM {self._json(tbl='o', col='o_orderdate', dts=dts)}) AS o_year,
                        p_l_joined.l_extendedprice * (1  - p_l_joined.l_discount) AS volume,
                        {self._json(tbl='n2', col='n_name', dts=dts)} AS nation
                FROM
                        (
                                SELECT
                                        {self._json(tbl='p', col='p_partkey', dts=dts)} AS p_partkey,
                                        {self._json(tbl='l', col='l_extendedprice', dts=dts)} AS l_extendedprice,
                                        {self._json(tbl='l', col='l_discount', dts=dts)} AS l_discount,
                                        {self._json(tbl='l', col='l_orderkey', dts=dts)} AS l_orderkey,
                                        {self._json(tbl='l', col='l_partkey', dts=dts)} AS l_partkey,
                                        {self._json(tbl='l', col='l_suppkey', dts=dts)} AS l_suppkey
                                FROM 
                                        extracted p,
                                        extracted l
                                WHERE
                                        {self._json(tbl='p', col='p_type', dts=dts)} = 'ECONOMY ANODIZED STEEL'
                                        AND {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='l', col='l_partkey', dts=dts)}

                        ) AS p_l_joined,
                        (
                                SELECT
                                        {self._json(tbl='n1', col='n_nationkey', dts=dts)} AS n_nationkey
                                FROM
                                        extracted r,
                                        extracted n1
                                WHERE
                                        {self._json(tbl='r', col='r_name', dts=dts)} = 'AMERICA'
                                        AND {self._json(tbl='r', col='r_regionkey', dts=dts)} = {self._json(tbl='n1', col='n_regionkey', dts=dts)}
                        ) AS r_n1_joined,
                        extracted o,
                        extracted c,
                        extracted s,
                        extracted n2
                WHERE

                        {self._json(tbl='s', col='s_suppkey', dts=dts)} = p_l_joined.l_suppkey
                        AND p_l_joined.l_orderkey = {self._json(tbl='o', col='o_orderkey', dts=dts)}
                        AND {self._json(tbl='o', col='o_custkey', dts=dts)} = {self._json(tbl='c', col='c_custkey', dts=dts)}
                        AND {self._json(tbl='c', col='c_nationkey', dts=dts)} = r_n1_joined.n_nationkey
                        AND {self._json(tbl='s', col='s_nationkey', dts=dts)} = {self._json(tbl='n2', col='n_nationkey', dts=dts)}
                        AND {self._json(tbl='o', col='o_orderdate', dts=dts)} BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
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
                "p_type",
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

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "p_partkey": True,
            "l_partkey": False,
            "r_regionkey": True,
            "n_regionkey": False,
            "s_suppkey": False,
            "o_orderkey": True,
            "o_custkey": True,
            "c_custkey": False,
            "c_nationkey": False,
            "s_nationkey": False,
            "n_nationkey": False
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "p_type": True,
            "r_name": True,
            "o_orderdate": True
        }

        return field_map[field]
