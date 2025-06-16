from queries.query import Query


class Q21(Query):
    """
    TPC-H Query 21
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 21, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='s', col='s_name', fields=fields)} AS s_name,
    COUNT(*) AS numwait
FROM
    test_table s,
    test_table l1,
    test_table o,
    test_table n
WHERE
    {self._json(tbl='s', col='s_suppkey', fields=fields)} = {self._json(tbl='l1', col='l_suppkey', fields=fields)}
    AND {self._json(tbl='o', col='o_orderkey', fields=fields)} = {self._json(tbl='l1', col='l_orderkey', fields=fields)}
    AND {self._json(tbl='o', col='o_orderstatus', fields=fields)} = 'F'
    AND {self._json(tbl='l1', col='l_receiptdate', fields=fields)} > {self._json(tbl='l1', col='l_commitdate', fields=fields)}
    AND EXISTS (
        SELECT
            *
        FROM
            test_table l2
        WHERE
            {self._json(tbl='l2', col='l_orderkey', fields=fields)} = {self._json(tbl='l1', col='l_orderkey', fields=fields)}
            AND {self._json(tbl='l2', col='l_suppkey', fields=fields)} <> {self._json(tbl='l1', col='l_suppkey', fields=fields)}
    )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            test_table l3
        WHERE
            {self._json(tbl='l3', col='l_orderkey', fields=fields)} = {self._json(tbl='l1', col='l_orderkey', fields=fields)}
            AND {self._json(tbl='l3', col='l_suppkey', fields=fields)} <> {self._json(tbl='l1', col='l_suppkey', fields=fields)}
            AND {self._json(tbl='l3', col='l_receiptdate', fields=fields)} > {self._json(tbl='l3', col='l_commitdate', fields=fields)}
    )
    AND {self._json(tbl='s', col='s_nationkey', fields=fields)} = {self._json(tbl='n', col='n_nationkey', fields=fields)}
    AND {self._json(tbl='n', col='n_name', fields=fields)} = 'SAUDI ARABIA'
GROUP BY
    {self._json(tbl='s', col='s_name', fields=fields)}
ORDER BY
    numwait DESC,
    {self._json(tbl='s', col='s_name', fields=fields)}
LIMIT
    100;

    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 3

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
                "s_name"
            ],
            'where': [
                "o_orderstatus",
                "l_receiptdate",
                "l_commitdate",
                "l_receiptdate",
                "l_commitdate",
                "n_name"
            ],
            'group_by': [
                "s_name"
            ],
            'order_by': [
                "s_name"
            ],
            'join': {
                "s_suppkey": ["l_suppkey"],
                "l_suppkey": ["s_suppkey"],
                "o_orderkey": ["l_orderkey"],
                "l_orderkey": ["o_orderkey"],
                "s_nationkey": ["n_nationkey"],
                "n_nationkey": ["s_nationkey"]
            },
            "self_join": {
                "l_orderkey": 2,
                "l_suppkey": 2
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "s_suppkey": False,
            "l_suppkey": False,
            "o_orderkey": True,
            "l_orderkey": False,
            "s_nationkey": False,
            "n_nationkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "o_orderstatus": 1,
            "l_receiptdate": 0,
            "l_commitdate": 0,
            "n_name": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "s_suppkey": 1,
            "l_suppkey": 1,
            "o_orderkey": 0,
            "l_orderkey": 1,
            "s_nationkey": 1,
            "n_nationkey": 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
