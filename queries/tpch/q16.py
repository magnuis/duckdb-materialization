from queries.query import Query


class Q16(Query):
    """
    TPC-H Query 16
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 16, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='p', col='p_brand', dt=dts['p_brand'])} AS p_brand,
    {self._json(tbl='p', col='p_type', dt=dts['p_type'])} AS p_type,
    {self._json(tbl='p', col='p_size', dt=dts['p_size'])} AS p_size,
    COUNT(DISTINCT {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])}) AS supplier_cnt
FROM
    test_table ps,
    test_table p
WHERE
    {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])}
    AND {self._json(tbl='p', col='p_brand', dt=dts['p_brand'])} <> 'Brand#45'
    AND {self._json(tbl='p', col='p_type', dt=dts['p_type'])} NOT LIKE 'MEDIUM POLISHED%'
    AND {self._json(tbl='p', col='p_size', dt=dts['p_size'])} IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])} NOT IN (
        SELECT
            {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])}
        FROM
            test_table s
        WHERE
            {self._json(tbl='s', col='s_comment', dt=dts['s_comment'])} LIKE '%Customer%Complaints%'
    )
GROUP BY
    {self._json(tbl='p', col='p_brand', dt=dts['p_brand'])},
    {self._json(tbl='p', col='p_type', dt=dts['p_type'])},
    {self._json(tbl='p', col='p_size', dt=dts['p_size'])}
ORDER BY
    supplier_cnt DESC,
    {self._json(tbl='p', col='p_brand', dt=dts['p_brand'])},
    {self._json(tbl='p', col='p_type', dt=dts['p_type'])},
    {self._json(tbl='p', col='p_size', dt=dts['p_size'])};

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

    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """

        field_map = {
            "p_brand": True,
            "p_type": False,
            "p_size": True,
            "ps_suppkey": False,
            "s_comment": True
        }

        return field_map[field]
