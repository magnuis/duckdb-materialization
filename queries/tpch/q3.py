from queries.query import Query


class Q3(Query):
    """
    TPC-H Query 3
    """

    def __init__(self):
        super().__init__()

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 3, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} AS s_acctbal,
    SUM( {self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])})) AS revenue,
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} AS o_orderdate,
    {self._json(tbl='o', col='o_shippriority', dt=dts['o_shippriority'])} AS o_shippriority,
FROM
    test_table c,
    test_table o,
    test_table l
WHERE
    {self._json(tbl='c', col='c_mktsegment', dt=dts['c_mktsegment'])} = 'BUILDING'
    AND {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])} = {self._json(tbl='o', col='o_custkey', dt=dts['o_custkey'])}
    AND {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} = {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])}
    AND {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} < DATE '1995-03-15'
    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} > DATE '1995-03-15'
GROUP BY
    {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])},
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])},
    {self._json(tbl='o', col='o_shippriority', dt=dts['o_shippriority'])}
ORDER BY
    revenue DESC,
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])}
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

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """
        field_map = {
            "c_custkey": True,
            "o_custkey": True,
            "l_orderkey": True,
            "o_orderkey": True
        }

        return field_map[field]

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "c_mktsegment": 1,
            "o_orderdate": 1,
            "l_shipdate": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "c_custkey": 0,
            "o_custkey": 0,
            "l_orderkey": 0,
            "o_orderkey": 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
