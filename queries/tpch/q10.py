from queries.query import Query


class Q10(Query):
    """
    TPC-H Query 10
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 10, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='c', col='c_custkey', fields=fields)} AS c_custkey,
    {self._json(tbl='c', col='c_name', fields=fields)} AS c_name,
    SUM({self._json(tbl='l', col='l_extendedprice', fields=fields)} * (1 - {self._json(tbl='l', col='l_discount', fields=fields)})) AS revenue,
    {self._json(tbl='c', col='c_acctbal', fields=fields)} AS c_acctbal,
    {self._json(tbl='n', col='n_name', fields=fields)} AS n_name,
    {self._json(tbl='c', col='c_address', fields=fields)} AS c_address,
    {self._json(tbl='c', col='c_phone', fields=fields)} AS c_phone,
    {self._json(tbl='c', col='c_comment', fields=fields)} AS c_comment
FROM
    test_table c,
    test_table o,
    test_table l,
    test_table n
WHERE
    {self._json(tbl='c', col='c_custkey', fields=fields)} = {self._json(tbl='o', col='o_custkey', fields=fields)}
    AND {self._json(tbl='l', col='l_orderkey', fields=fields)} = {self._json(tbl='o', col='o_orderkey', fields=fields)}
    AND {self._json(tbl='o', col='o_orderdate', fields=fields)} >= DATE '1993-10-01'
    AND {self._json(tbl='o', col='o_orderdate', fields=fields)} < DATE '1994-01-01'
    AND {self._json(tbl='l', col='l_returnflag', fields=fields)} = 'R'
    AND {self._json(tbl='c', col='c_nationkey', fields=fields)} = {self._json(tbl='n', col='n_nationkey', fields=fields)}
GROUP BY
    {self._json(tbl='c', col='c_custkey', fields=fields)},
    {self._json(tbl='c', col='c_name', fields=fields)},
    {self._json(tbl='c', col='c_acctbal', fields=fields)},
    {self._json(tbl='c', col='c_phone', fields=fields)},
    {self._json(tbl='n', col='n_name', fields=fields)},
    {self._json(tbl='c', col='c_address', fields=fields)},
    {self._json(tbl='c', col='c_comment', fields=fields)}
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

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "c_custkey": False,
            "o_custkey": True,
            "l_orderkey": True,
            "o_orderkey": True,
            "c_nationkey": False,
            "n_nationkey": False
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "o_orderdate": 1,
            "l_returnflag": 1
        }

        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "c_custkey": 1,
            "o_custkey": 0,
            "l_orderkey": 0,
            "o_orderkey": 0,
            "c_nationkey": 1,
            "n_nationkey": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
