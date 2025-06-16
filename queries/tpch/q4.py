from queries.query import Query


class Q4(Query):
    """
    TPC-H Query 4
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 4, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='o', col='o_orderpriority', fields=fields)} AS o_orderpriority,
    COUNT(*) AS order_count
FROM
    test_table o
WHERE
    {self._json(tbl='o', col='o_orderdate', fields=fields)} >= DATE '1993-07-01'
    AND {self._json(tbl='o', col='o_orderdate', fields=fields)} < DATE '1993-07-01' + INTERVAL '3' MONTH
    AND EXISTS (
        SELECT
            *
        FROM
            test_table l
        WHERE
            {self._json(tbl='l', col='l_orderkey', fields=fields)} = {self._json(tbl='o', col='o_orderkey', fields=fields)}
            AND {self._json(tbl='l', col='l_commitdate', fields=fields)} < {self._json(tbl='l', col='l_receiptdate', fields=fields)}
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

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "l_orderkey": True,
            "o_orderkey": True,
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "o_orderdate": 1,
            "l_commitdate": 0,
            "l_receiptdate": 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """

        field_map = {
            "l_orderkey": 0,
            "o_orderkey": 0,
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
