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

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 16

        Returns
        -------
        list[str]
        """

        return [
            "p_brand",
            "p_type",
            "p_size",
            "ps_suppkey",
            "p_partkey",
            "ps_partkey",
            "s_suppkey",
            "s_comment"
        ]
