from queries.query import Query


class Q9(Query):
    """
    TPC-H Query 9
    """

    def __init__(self):
        pass

#     def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
#         """
#         Get the formatted TPC-H query 9, adjusted to current db materializaiton

#         Returns
#         -------
#         str
#         """

#         dts = self._get_field_accesses(fields=fields)

#         return f"""
# SELECT
#     nation,
#     o_year,
#     SUM(amount) AS sum_profit
# FROM
#     (
#         SELECT
#             {self._json(tbl='n', col='n_name', dt=dts['n_name'])} AS nation,
#             EXTRACT(YEAR FROM {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])}) AS o_year,
#             {self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])}) - {self._json(tbl='ps', col='ps_supplycost', dt=dts['ps_supplycost'])} * {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])} AS amount
#         FROM
#             test_table p,
#             test_table s,
#             test_table l,
#             test_table ps,
#             test_table o,
#             test_table n
#         WHERE
#             {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])}
#             AND {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])} = {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])}
#             AND {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
#             AND {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
#             AND {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])} = {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])}
#             AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
#             AND {self._json(tbl='p', col='p_name', dt=dts['p_name'])} LIKE '%green%'
#     ) AS profit
# GROUP BY
#     nation,
#     o_year
# ORDER BY
#     nation,
#     o_year DESC;
#     """

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 9, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
    SELECT
            nation,
            o_year,
            SUM(amount) AS sum_profit
    FROM
            (
                    SELECT
                            {self._json(tbl='n', col='n_name', dt=dts['n_name'])}  AS nation,
                            EXTRACT(YEAR FROM {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])})  AS o_year,
                            lp_joined.l_extendedprice * (1 - lp_joined.l_discount) - {self._json(tbl='ps', col='ps_supplycost', dt=dts['ps_supplycost'])} * lp_joined.l_discount AS amount
                    FROM
                            (
                                    select 
                                            {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])} AS l_partkey,
                                            {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])} AS l_suppkey,
                                            {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} AS l_orderkey,
                                            {self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} AS l_extendedprice,
                                            {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])} AS l_discount

                                    FROM
                                            test_table p,
                                            test_table l
                                    WHERE
                                            {self._json(tbl='p', col='p_name', dt=dts['p_name'])} like '%green%'
                                            AND {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
                            ) AS lp_joined,
                            test_table s,
                            test_table ps,
                            test_table o,
                            test_table n
                    WHERE
                            {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = lp_joined.l_suppkey
                            AND {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])} = lp_joined.l_suppkey
                            AND {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])} = lp_joined.l_partkey
                            AND {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])} = lp_joined.l_orderkey
                            AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
            ) AS profit
    GROUP BY
            nation,
            o_year
    ORDER BY
            nation,
            o_year desc;

    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 6

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
                "n_name",
                "o_orderdate",
                "ps_supplycost",
                "l_partkey",
                "l_suppkey",
                "l_orderkey",
                "l_extendedprice",
                "l_dscount"
            ],
            'where': [
                "p_name"
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': [
                "p_partkey",
                "l_partkey",
                "s_suppkey",
                "ps_suppkey",
                "ps_partkey",
                "s_nationkey",
                "n_nationkey"
            ]
        }
