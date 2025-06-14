from queries.query import Query


class Q12(Query):
    """
    TPC-H Query 12
    """

    def __init__(self):
        super().__init__()

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 12, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])} AS l_shipmode,
    SUM(CASE
            WHEN {self._json(tbl='o', col='o_orderpriority', dt=dts['o_orderpriority'])} = '1-URGENT' OR {self._json(tbl='o', col='o_orderpriority', dt=dts['o_orderpriority'])} = '2-HIGH' THEN 1
            ELSE 0
        END) AS high_line_count,
    SUM(CASE
            WHEN {self._json(tbl='o', col='o_orderpriority', dt=dts['o_orderpriority'])} <> '1-URGENT' AND {self._json(tbl='o', col='o_orderpriority', dt=dts['o_orderpriority'])} <> '2-HIGH' THEN 1
            ELSE 0
        END) AS low_line_count
FROM
    test_table o,
    test_table l
WHERE
    {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])} = {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])}
    AND {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])} IN ('MAIL', 'SHIP')
    AND {self._json(tbl='l', col='l_commitdate', dt=dts['l_commitdate'])}  < {self._json(tbl='l', col='l_receiptdate', dt=dts['l_receiptdate'])}
    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])}  < {self._json(tbl='l', col='l_commitdate', dt=dts['l_commitdate'])}
    AND {self._json(tbl='l', col='l_receiptdate', dt=dts['l_receiptdate'])} >= DATE '1994-01-01'
    AND {self._json(tbl='l', col='l_receiptdate', dt=dts['l_receiptdate'])} < DATE '1995-01-01'
GROUP BY
    {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])}
ORDER BY
    {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])};
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 1

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
                "l_shipmode",
                "o_orderpriority"
            ],
            'where': [
                "l_shipmode",
                "l_commitdate",
                "l_receiptdate",
                "l_shipdate"
            ],
            'group_by': [
                "l_shipmode"
            ],
            'order_by': [
                "l_shipmode"
            ],
            'join': {
                "o_orderkey": ["l_orderkey"],
                "l_orderkey": ["o_orderkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "o_orderkey": False,
            "l_orderkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "l_shipmode": 1,
            "l_commitdate": 0,
            "l_receiptdate": 1,
            "l_shipdate": 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")

        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "o_orderkey": 1,
            "l_orderkey": 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
