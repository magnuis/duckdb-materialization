from queries.query import Query


class Q10(Query):
    """
    TPC-H Query 10
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "c": ["c_custkey", "c_name", "c_acctbal", "c_address", "c_phone", "c_comment", "c_nationkey"],
            "o": ["o_orderdate", "o_custkey", "o_orderkey"],
            "l": ["l_extendedprice", "l_discount", "l_returnflag", "l_orderkey"],
            "n": ["n_name", "n_nationkey"]
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 10, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='c', col='c_custkey', dts=dts)} AS c_custkey,
    {self._json(tbl='c', col='c_name', dts=dts)} AS c_name,
    SUM({self._json(tbl='l', col='l_extendedprice', dts=dts)} * (1 - {self._json(tbl='l', col='l_discount', dts=dts)})) AS revenue,
    {self._json(tbl='c', col='c_acctbal', dts=dts)} AS c_acctbal,
    {self._json(tbl='n', col='n_name', dts=dts)} AS n_name,
    {self._json(tbl='c', col='c_address', dts=dts)} AS c_address,
    {self._json(tbl='c', col='c_phone', dts=dts)} AS c_phone,
    {self._json(tbl='c', col='c_comment', dts=dts)} AS c_comment
FROM
    extracted c,
    extracted o,
    extracted l,
    extracted n
WHERE
    {self._json(tbl='c', col='c_custkey', dts=dts)} = {self._json(tbl='o', col='o_custkey', dts=dts)}
    AND {self._json(tbl='l', col='l_orderkey', dts=dts)} = {self._json(tbl='o', col='o_orderkey', dts=dts)}
    AND {self._json(tbl='o', col='o_orderdate', dts=dts)} >= DATE '1993-10-01'
    AND {self._json(tbl='o', col='o_orderdate', dts=dts)} < DATE '1994-01-01'
    AND {self._json(tbl='l', col='l_returnflag', dts=dts)} = 'R'
    AND {self._json(tbl='c', col='c_nationkey', dts=dts)} = {self._json(tbl='n', col='n_nationkey', dts=dts)}
GROUP BY
    {self._json(tbl='c', col='c_custkey', dts=dts)},
    {self._json(tbl='c', col='c_name', dts=dts)},
    {self._json(tbl='c', col='c_acctbal', dts=dts)},
    {self._json(tbl='c', col='c_phone', dts=dts)},
    {self._json(tbl='n', col='n_name', dts=dts)},
    {self._json(tbl='c', col='c_address', dts=dts)},
    {self._json(tbl='c', col='c_comment', dts=dts)}
ORDER BY
    revenue DESC
LIMIT
    20;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 3

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
                "c_custkey",
                "c_name",
                "l_extendedprice",
                "l_discount",
                "c_acctbal",
                "n_name",
                "c_address",
                "c_phone",
                "c_comment"
            ],
            'where': [
                "o_orderdate",
                "l_returnflag"
            ],
            'group_by': [
                "c_custkey",
                "c_name",
                "c_acctbal",
                "c_phone",
                "n_name",
                "c_address",
                "c_comment"
            ],
            'order_by': [
            ],
            'join': {
                "c_custkey": ["o_custkey"],
                "o_custkey": ["c_custkey"],
                "l_orderkey": ["o_orderkey"],
                "o_orderkey": ["l_orderkey"],
                "c_nationkey": ["n_nationkey"],
                "n_nationkey": ["c_nationkey"]
            }
        }
