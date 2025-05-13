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

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "ps": ["ps_supplycost", "ps_partkey", "ps_suppkey"],
            "l": ["l_partkey", "l_suppkey", "l_orderkey", "l_extendedprice", "l_discount"],
            "o": ["o_orderdate", "o_orderkey"],
            "p": ["p_name", "p_partkey"],
            "n": ["n_name", "n_nationkey"],
            "s": ["s_suppkey", "s_nationkey"]
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 9, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
    SELECT
            nation,
            o_year,
            SUM(amount) AS sum_profit
    FROM
            (
                    SELECT
                            {self._json(tbl='n', col='n_name', dts=dts)}  AS nation,
                            EXTRACT(YEAR FROM {self._json(tbl='o', col='o_orderdate', dts=dts)})  AS o_year,
                            lp_joined.l_extendedprice * (1 - lp_joined.l_discount) - {self._json(tbl='ps', col='ps_supplycost', dts=dts)} * lp_joined.l_discount AS amount
                    FROM
                            (
                                    select 
                                            {self._json(tbl='l', col='l_partkey', dts=dts)} AS l_partkey,
                                            {self._json(tbl='l', col='l_suppkey', dts=dts)} AS l_suppkey,
                                            {self._json(tbl='l', col='l_orderkey', dts=dts)} AS l_orderkey,
                                            {self._json(tbl='l', col='l_extendedprice', dts=dts)} AS l_extendedprice,
                                            {self._json(tbl='l', col='l_discount', dts=dts)} AS l_discount

                                    FROM
                                            extracted p,
                                            extracted l
                                    WHERE
                                            {self._json(tbl='p', col='p_name', dts=dts)} like '%green%'
                                            AND {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='l', col='l_partkey', dts=dts)}
                            ) AS lp_joined,
                            extracted s,
                            extracted ps,
                            extracted o,
                            extracted n
                    WHERE
                            {self._json(tbl='s', col='s_suppkey', dts=dts)} = lp_joined.l_suppkey
                            AND {self._json(tbl='ps', col='ps_suppkey', dts=dts)} = lp_joined.l_suppkey
                            AND {self._json(tbl='ps', col='ps_partkey', dts=dts)} = lp_joined.l_partkey
                            AND {self._json(tbl='o', col='o_orderkey', dts=dts)} = lp_joined.l_orderkey
                            AND {self._json(tbl='s', col='s_nationkey', dts=dts)} = {self._json(tbl='n', col='n_nationkey', dts=dts)}
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
                "l_discount"
            ],
            'where': [
                "p_name"
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {
                "p_partkey": ["l_partkey"],
                "l_partkey": ["p_partkey"],
                "s_suppkey": [None],
                "ps_suppkey": [None],
                "ps_partkey": [None],
                "s_nationkey": ["n_nationkey"],
                "n_nationkey": ["s_nationkey"],
                "o_orderkey": [None]
            }
        }
