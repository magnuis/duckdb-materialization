from queries.query import Query


class Q20(Query):
    """
    TPC-H Query 20
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 20, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='s', col='s_name', fields=fields)} AS s_name,
    {self._json(tbl='s', col='s_address', fields=fields)} AS s_address
FROM
    test_table s,
    test_table n
WHERE
    {self._json(tbl='s', col='s_suppkey', fields=fields)} IN (
        SELECT
            {self._json(tbl='ps', col='ps_suppkey', fields=fields)}
        FROM
            test_table ps
        WHERE
            {self._json(tbl='ps', col='ps_partkey', fields=fields)} IN (
                SELECT
                    {self._json(tbl='p', col='p_partkey', fields=fields)}
                FROM
                    test_table p
                WHERE
                    {self._json(tbl='p', col='p_name', fields=fields)} LIKE 'forest%'
            )
            AND {self._json(tbl='ps', col='ps_availqty', fields=fields)} > (
                SELECT
                    0.5 * SUM({self._json(tbl='l', col='l_quantity', fields=fields)})
                FROM
                    test_table l
                WHERE
                    {self._json(tbl='l', col='l_partkey', fields=fields)} = {self._json(tbl='ps', col='ps_partkey', fields=fields)}
                    AND {self._json(tbl='l', col='l_suppkey', fields=fields)} = {self._json(tbl='ps', col='ps_suppkey', fields=fields)}
                    AND {self._json(tbl='l', col='l_shipdate', fields=fields)} >= DATE '1994-01-01'
                    AND {self._json(tbl='l', col='l_shipdate', fields=fields)} < DATE '1995-01-01'
            )
    )
    AND {self._json(tbl='s', col='s_nationkey', fields=fields)} = {self._json(tbl='n', col='n_nationkey', fields=fields)}
    AND {self._json(tbl='n', col='n_name', fields=fields)} = 'CANADA'
ORDER BY
    {self._json(tbl='s', col='s_name', fields=fields)};
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
                "s_name",
                "s_address",
                "ps_suppkey",
                "p_partkey",
                "l_quantity"
            ],
            'where': [
                "s_suppkey",
                "ps_partkey",
                "p_name",
                "ps_availqty",
                "l_shipdate",
                "n_name",
            ],
            'group_by': [],
            'order_by': [
                "s_name"
            ],
            'join': {
                "l_partkey": ["ps_partkey"],
                "ps_partkey": ["l_partkey"],
                "l_suppkey": ["ps_suppkey"],
                "ps_suppkey": ["l_suppkey"],
                "s_nationkey": ["n_nationkey"],
                "n_nationkey": ["s_nationkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "l_partkey": True,
            "ps_partkey": False,
            "l_suppkey": True,
            "ps_suppkey": False,
            "s_nationkey": False,
            "n_nationkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "s_suppkey": 0,
            "ps_partkey": 0,
            "p_name": 1,
            "ps_availqty": 0,
            "l_shipdate": 1,
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
            "l_partkey": 0,
            "ps_partkey": 1,
            "l_suppkey": 0,
            "ps_suppkey": 1,
            "s_nationkey": 1,
            "n_nationkey": 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
