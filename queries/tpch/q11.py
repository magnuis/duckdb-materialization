from queries.query import Query


class Q11(Query):
    """
    TPC-H Query 11
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 11, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='ps', col='ps_partkey', fields=fields)} AS ps_partkey,
    SUM({self._json(tbl='ps', col='ps_supplycost', fields=fields)} * {self._json(tbl='ps', col='ps_availqty', fields=fields)}) AS value
FROM
    test_table ps,
    test_table s,
    test_table n
WHERE
    {self._json(tbl='ps', col='ps_suppkey', fields=fields)} = {self._json(tbl='s', col='s_suppkey', fields=fields)}
    AND {self._json(tbl='s', col='s_nationkey', fields=fields)} = {self._json(tbl='n', col='n_nationkey', fields=fields)}
    AND {self._json(tbl='n', col='n_name', fields=fields)} = 'GERMANY'
GROUP BY
    {self._json(tbl='ps', col='ps_partkey', fields=fields)}
HAVING
    SUM({self._json(tbl='ps', col='ps_supplycost', fields=fields)} * {self._json(tbl='ps', col='ps_availqty', fields=fields)}) > (
        SELECT
            SUM({self._json(tbl='ps', col='ps_supplycost', fields=fields)} * {self._json(tbl='ps', col='ps_availqty', fields=fields)}) * 0.0001
        FROM
            test_table ps,
            test_table s,
            test_table n
        WHERE
            {self._json(tbl='ps', col='ps_suppkey', fields=fields)} = {self._json(tbl='s', col='s_suppkey', fields=fields)}
            AND {self._json(tbl='s', col='s_nationkey', fields=fields)} = {self._json(tbl='n', col='n_nationkey', fields=fields)}
            AND {self._json(tbl='n', col='n_name', fields=fields)} = 'GERMANY'
    )
ORDER BY
    value DESC,
    ps_partkey;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 4

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
                "ps_partkey",
                "ps_supplycost",
                "ps_availqty",
                "ps_supplycost",
                "ps_availqty"
            ],
            'where': [
                "ps_supplycost",
                "ps_availqty",
                "n_name",
                "n_name"
            ],
            'group_by': [
                "ps_partkey"
            ],
            'order_by': [
            ],
            'join': {
                "ps_suppkey": ["s_suppkey", "s_suppkey"],
                "s_suppkey": ["ps_suppkey", "ps_suppkey"],
                "s_nationkey": ["n_nationkey", "n_nationkey"],
                "n_nationkey": ["s_nationkey", "s_nationkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "ps_suppkey": False,
            "s_suppkey": False,
            "s_nationkey": False,
            "n_nationkey": True,
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "ps_supplycost": 0,
            "ps_availqty": 0,
            "n_name": 2,
        }
        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")

        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "ps_suppkey": 2,
            "s_suppkey": 2,
            "s_nationkey": 2,
            "n_nationkey": 0,
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
