from queries.query import Query


class Q4(Query):
    """
    TPC-H Query 4
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 4, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='o', col='o_orderpriority', dt=dts['o_orderpriority'])} AS o_orderpriority,
    COUNT(*) AS order_count
FROM
    test_table o
WHERE
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} >= DATE '1993-07-01'
    AND {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} < DATE '1993-07-01' + INTERVAL '3' MONTH
    AND EXISTS (
        SELECT
            *
        FROM
            test_table l
        WHERE
            {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} = {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])}
            AND {self._json(tbl='l', col='l_commitdate', dt=dts['l_commitdate'])} < {self._json(tbl='l', col='l_receiptdate', dt=dts['l_receiptdate'])}
    )
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;

    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 1

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
                "o_orderpriority"
            ],
            'where': [
                "o_orderdate",
                "l_commitdate",
                "l_receiptdate"
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {
                "l_orderkey": ["o_orderkey"],
                "o_orderkey": ["l_orderkey"]
            }
        }

    def join_field_has_filter(self, field: str) -> bool | None:
        """
        Check if the table of the the join field has a filter
        """
        assert field in self.columns_used()

        field_map = {
            "l_orderkey": False,
            "o_orderkey": False,
        }

        return field_map.get(field, False)
