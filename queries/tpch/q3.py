from queries.query import Query


class Q3(Query):
    """
    TPC-H Query 3
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "c": ["c_mktsegment", "c_custkey"],
            "l": ["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"],
            "o": ["o_orderdate", "o_custkey", "o_orderkey", "o_shippriority"]
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 3, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='l', col='l_orderkey', dts=dts)} AS s_acctbal,
    SUM( {self._json(tbl='l', col='l_extendedprice', dts=dts)} * (1 - {self._json(tbl='l', col='l_discount', dts=dts)})) AS revenue,
    {self._json(tbl='o', col='o_orderdate', dts=dts)} AS o_orderdate,
    {self._json(tbl='o', col='o_shippriority', dts=dts)} AS o_shippriority,
FROM
    extracted c,
    extracted o,
    extracted l
WHERE
    {self._json(tbl='c', col='c_mktsegment', dts=dts)} = 'BUILDING'
    AND {self._json(tbl='c', col='c_custkey', dts=dts)} = {self._json(tbl='o', col='o_custkey', dts=dts)}
    AND {self._json(tbl='l', col='l_orderkey', dts=dts)} = {self._json(tbl='o', col='o_orderkey', dts=dts)}
    AND {self._json(tbl='o', col='o_orderdate', dts=dts)} < DATE '1995-03-15'
    AND {self._json(tbl='l', col='l_shipdate', dts=dts)} > DATE '1995-03-15'
GROUP BY
    {self._json(tbl='l', col='l_orderkey', dts=dts)},
    {self._json(tbl='o', col='o_orderdate', dts=dts)},
    {self._json(tbl='o', col='o_shippriority', dts=dts)}
ORDER BY
    revenue DESC,
    {self._json(tbl='o', col='o_orderdate', dts=dts)}
LIMIT
    10;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 2

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
                "l_orderkey",
                "l_extendedprice",
                "l_discount",
                "o_orderdate",
                "o_shippriority"
            ],
            'where': [
                "c_mktsegment",
                "o_orderdate",
                "l_shipdate"
            ],
            'group_by': [
                "l_orderkey",
                "o_orderdate",
                "o_shippriority"
            ],
            'order_by': [
                "o_orderdate"
            ],
            'join':
            {
                "c_custkey": ["o_custkey"],
                "o_custkey": ["c_custkey"],
                "l_orderkey": ["o_orderkey"],
                "o_orderkey": ["l_orderkey"]
            }
        }
