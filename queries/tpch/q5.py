from queries.query import Query


class Q5(Query):
    """
    TPC-H Query 5
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 5, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='n', col='n_name', fields=fields)} AS n_name,
    SUM({self._json(tbl='l', col='l_extendedprice', fields=fields)} * (1 - {self._json(tbl='l', col='l_discount', fields=fields)})) AS revenue
FROM
    test_table c,
    test_table o,
    test_table l,
    test_table s,
    test_table n,
    test_table r
WHERE
    {self._json(tbl='c', col='c_custkey', fields=fields)} = {self._json(tbl='o', col='o_custkey', fields=fields)}
    AND {self._json(tbl='l', col='l_orderkey', fields=fields)} = {self._json(tbl='o', col='o_orderkey', fields=fields)}
    AND {self._json(tbl='l', col='l_suppkey', fields=fields)} = {self._json(tbl='s', col='s_suppkey', fields=fields)}
    AND {self._json(tbl='c', col='c_nationkey', fields=fields)} = {self._json(tbl='s', col='s_nationkey', fields=fields)}
    AND {self._json(tbl='s', col='s_nationkey', fields=fields)} = {self._json(tbl='n', col='n_nationkey', fields=fields)}
    AND {self._json(tbl='n', col='n_regionkey', fields=fields)} = {self._json(tbl='r', col='r_regionkey', fields=fields)}
    AND {self._json(tbl='r', col='r_name', fields=fields)}= 'ASIA'
    AND {self._json(tbl='o', col='o_orderdate', fields=fields)} >= DATE '1994-01-01'
    AND {self._json(tbl='o', col='o_orderdate', fields=fields)} < DATE '1995-01-01'
GROUP BY
    {self._json(tbl='n', col='n_name', fields=fields)}
ORDER BY
    revenue DESC;

    """

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
            'order_by': [],
            'join': {
                "c_custkey": ["o_custkey"],
                "o_custkey": ["c_custkey"],
                "l_orderkey": ["o_orderkey"],
                "o_orderkey": ["l_orderkey"],
                "l_suppkey": ["o_suppkey"],
                "s_suppkey": ["l_suppkey"],
                "c_nationkey": ["s_nationkey"],
                "s_nationkey": ["c_nationkey", "n_nationkey"],
                "n_nationkey": ["s_nationkey"],
                "n_regionkey": ["r_regionkey"],
                "r_regionkey": ["n_regionkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "c_custkey": False,
            "o_custkey": True,
            "l_orderkey": False,
            "o_orderkey": True,
            "l_suppkey": False,
            "s_suppkey": False,
            "c_nationkey": False,
            "s_nationkey": False,
            "n_nationkey": False,
            "n_regionkey": False,
            "r_regionkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "r_name": True,
            "o_orderdate": True
        }

        return field_map[field]
