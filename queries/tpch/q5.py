from queries.query import Query


class Q5(Query):
    """
    TPC-H Query 5
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 5, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='n', col='n_name', dt=dts['n_name'])} AS n_name,
    SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])})) AS revenue
FROM
    test_table c,
    test_table o,
    test_table l,
    test_table s,
    test_table n,
    test_table r
WHERE
    {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])} = {self._json(tbl='o', col='o_custkey', dt=dts['o_custkey'])}
    AND {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} = {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])}
    AND {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])} = {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])}
    AND {self._json(tbl='c', col='c_nationkey', dt=dts['c_nationkey'])} = {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])}
    AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
    AND {self._json(tbl='n', col='n_regionkey', dt=dts['n_regionkey'])} = {self._json(tbl='r', col='r_regionkey', dt=dts['r_regionkey'])}
    AND {self._json(tbl='r', col='r_name', dt=dts['r_name'])}= 'ASIA'
    AND {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} >= DATE '1994-01-01'
    AND {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} < DATE '1995-01-01'
GROUP BY
    {self._json(tbl='n', col='n_name', dt=dts['n_name'])}
ORDER BY
    revenue DESC;

    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 5

        Returns
        -------
        list[str]
        """

        return [
            "n_name",
            "l_extendedprice",
            "l_discount",
            "c_custkey",
            "o_custkey",
            "l_orderkey",
            "o_orderkey",
            "l_suppkey",
            "s_suppkey",
            "c_nationkey",
            "s_nationkey",
            "n_nationkey",
            "n_regionkey",
            "r_regionkey",
            "r_name",
            "o_orderdate"
        ]

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 6

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """
        Get the columns used in the query along with their position in the query 
        (e.g., SELECT, WHERE, GROUP BY, ORDER BY clauses).

        Returns
        -------
        dict
            A dictionary with the following keys:
            - 'select': list of column names used in the SELECT clause.
            - 'where': list of column names used in the WHERE clause that are not joins.
            - 'group_by': list of column names used in the GROUP BY clause.
            - 'order_by': list of column names used in the ORDER BY clause.
            - 'join': list of column names used in a join operation (including WHERE)
        """
        return {
            'select': [
                "n_name",
                "l_extendedprice",
                "l_discount"
            ],
            'where': [
                "r_name",
                "o_orderdate"
            ],
            'group_by': [
                "n_name"
            ],
            'order_by': [
                "revenue"
            ],
            'join': [
                "c_custkey",
                "o_custkey",
                "l_orderkey",
                "o_orderkey",
                "l_suppkey",
                "s_suppkey",
                "c_nationkey",
                "s_nationkey",
                "n_nationkey",
                "n_regionkey",
                "r_regionkey"
            ]
        }
