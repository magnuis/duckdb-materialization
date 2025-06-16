from queries.query import Query


class Q16(Query):
    """
    TPC-H Query 16
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 16, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='p', col='p_brand', fields=fields)} AS p_brand,
    {self._json(tbl='p', col='p_type', fields=fields)} AS p_type,
    {self._json(tbl='p', col='p_size', fields=fields)} AS p_size,
    COUNT(DISTINCT {self._json(tbl='ps', col='ps_suppkey', fields=fields)}) AS supplier_cnt
FROM
    test_table ps,
    test_table p
WHERE
    {self._json(tbl='p', col='p_partkey', fields=fields)} = {self._json(tbl='ps', col='ps_partkey', fields=fields)}
    AND {self._json(tbl='p', col='p_brand', fields=fields)} <> 'Brand#45'
    AND {self._json(tbl='p', col='p_type', fields=fields)} NOT LIKE 'MEDIUM POLISHED%'
    AND {self._json(tbl='p', col='p_size', fields=fields)} IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND {self._json(tbl='ps', col='ps_suppkey', fields=fields)} NOT IN (
        SELECT
            {self._json(tbl='s', col='s_suppkey', fields=fields)}
        FROM
            test_table s
        WHERE
            {self._json(tbl='s', col='s_comment', fields=fields)} LIKE '%Customer%Complaints%'
    )
GROUP BY
    {self._json(tbl='p', col='p_brand', fields=fields)},
    {self._json(tbl='p', col='p_type', fields=fields)},
    {self._json(tbl='p', col='p_size', fields=fields)}
ORDER BY
    supplier_cnt DESC,
    {self._json(tbl='p', col='p_brand', fields=fields)},
    {self._json(tbl='p', col='p_type', fields=fields)},
    {self._json(tbl='p', col='p_size', fields=fields)};

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
                "p_brand",
                "p_type",
                "p_size",
                "ps_suppkey",
                "s_suppkey",
            ],
            'where': [
                "p_brand",
                "p_type",
                "p_size",
                "ps_suppkey",
                "s_comment"
            ],
            'group_by': [
                "p_brand",
                "p_type",
                "p_size"
            ],
            'order_by': [
                "p_brand",
                "p_type",
                "p_size"
            ],
            'join': {
                "p_partkey": ["ps_partkey"],
                "ps_partkey": ["p_partkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "p_partkey": True,
            "ps_partkey": False
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query specific implementation of the where field has direct filter
        """

        field_map = {
            "p_brand": 1,
            "p_type": 0,
            "p_size": 1,
            "ps_suppkey": 0,
            "s_comment": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "p_partkey": 0,
            "ps_partkey": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
