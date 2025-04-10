from queries.query import Query


class Q15(Query):
    """
    TPC-H Query 15s
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 15s
, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
WITH revenue (supplier_no, total_revenue) AS (
    SELECT
        {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])} AS l_suppkey,
        SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])})) AS total_revenue
    FROM
        test_table l
    WHERE
        {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} >= DATE '1996-01-01'
        AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} < DATE '1996-04-01'
    GROUP BY
        {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])}
)
SELECT
    {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} AS s_suppkey,
    {self._json(tbl='s', col='s_name', dt=dts['s_name'])} AS s_name,
    {self._json(tbl='s', col='s_address', dt=dts['s_address'])} AS s_address,
    {self._json(tbl='s', col='s_phone', dt=dts['s_phone'])} AS s_phone,
    total_revenue
FROM
    test_table s,
    revenue
WHERE
    {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = revenue.supplier_no
    AND total_revenue = (
        SELECT
            MAX(total_revenue)
        FROM
            revenue
    )
ORDER BY
    {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])};

    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 15s

        Returns
        -------
        list[str]
        """

        return [
            "l_suppkey",
            "l_extendedprice",
            "l_discount",
            "l_shipdate",
            "supplier_no",
            "total_revenue",
            "s_suppkey",
            "s_name",
            "s_address",
            "s_phone"
        ]
