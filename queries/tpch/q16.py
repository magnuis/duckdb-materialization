from queries.query import Query


class Q16(Query):
    """
    TPC-H Query 16
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "s": ["s_comment", "s_suppkey"],
            "ps": ["ps_suppkey", "ps_partkey"],
            "p": ["p_brand", "p_type", "p_size", "p_partkey"],
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 16, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='p', col='p_brand', dts=dts)} AS p_brand,
    {self._json(tbl='p', col='p_type', dts=dts)} AS p_type,
    {self._json(tbl='p', col='p_size', dts=dts)} AS p_size,
    COUNT(DISTINCT {self._json(tbl='ps', col='ps_suppkey', dts=dts)}) AS supplier_cnt
FROM
    extracted ps,
    extracted p
WHERE
    {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='ps', col='ps_partkey', dts=dts)}
    AND {self._json(tbl='p', col='p_brand', dts=dts)} <> 'Brand#45'
    AND {self._json(tbl='p', col='p_type', dts=dts)} NOT LIKE 'MEDIUM POLISHED%'
    AND {self._json(tbl='p', col='p_size', dts=dts)} IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND {self._json(tbl='ps', col='ps_suppkey', dts=dts)} NOT IN (
        SELECT
            {self._json(tbl='s', col='s_suppkey', dts=dts)}
        FROM
            extracted s
        WHERE
            {self._json(tbl='s', col='s_comment', dts=dts)} LIKE '%Customer%Complaints%'
    )
GROUP BY
    {self._json(tbl='p', col='p_brand', dts=dts)},
    {self._json(tbl='p', col='p_type', dts=dts)},
    {self._json(tbl='p', col='p_size', dts=dts)}
ORDER BY
    supplier_cnt DESC,
    {self._json(tbl='p', col='p_brand', dts=dts)},
    {self._json(tbl='p', col='p_type', dts=dts)},
    {self._json(tbl='p', col='p_size', dts=dts)};

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
