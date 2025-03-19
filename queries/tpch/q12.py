from queries.query import Query


class Q12(Query):
    """
    TPC-H Query 12
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 12, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])} AS l_shipmode,
    SUM(CASE
            WHEN {self._json(tbl='o', col='o_orderpriority', dt=dts['o_orderpriority'])} = '1-URGENT' OR {self._json(tbl='o', col='o_orderpriority', dt=dts['o_orderpriority'])} = '2-HIGH' THEN 1
            ELSE 0
        END) AS high_line_count,
    SUM(CASE
            WHEN {self._json(tbl='o', col='o_orderpriority', dt=dts['o_orderpriority'])} <> '1-URGENT' AND {self._json(tbl='o', col='o_orderpriority', dt=dts['o_orderpriority'])} <> '2-HIGH' THEN 1
            ELSE 0
        END) AS low_line_count
FROM
    test_table o,
    test_table l
WHERE
    {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])} = {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])}
    AND {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])} IN ('MAIL', 'SHIP')
    AND {self._json(tbl='l', col='l_commitdate', dt=dts['l_commitdate'])}  < {self._json(tbl='l', col='l_receiptdate', dt=dts['l_receiptdate'])}
    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])}  < {self._json(tbl='l', col='l_commitdate', dt=dts['l_commitdate'])}
    AND {self._json(tbl='l', col='l_receiptdate', dt=dts['l_receiptdate'])} >= DATE '1994-01-01'
    AND {self._json(tbl='l', col='l_receiptdate', dt=dts['l_receiptdate'])} < DATE '1995-01-01'
GROUP BY
    {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])}
ORDER BY
    {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])};
    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 12

        Returns
        -------
        list[str]
        """

        return [
            "l_shipmode",
            "o_orderpriority",
            "o_orderkey",
            "l_orderkey",
            "l_commitdate",
            "l_receiptdate",
            "l_shipdate"
        ]
