from queries.query import Query


class Q4(Query):
    """
    TPC-H Query 4
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 4, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        # TODO use more performant e.g. dict for loopup

        return f"""
SELECT
    {self._json(tbl='o', col='o_orderpriority', dt=dts['o_orderpriority'])} AS o_orderpriority,
    COUNT(*) AS order_count
FROM
    test_table o
WHERE
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} >= DATE '1993-07-01'
    AND {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} < DATE '1993-07-01' + INTERVAL '3' MONTH
    AND EXISTS (
        SELECT
            *
        FROM
            test_table l
        WHERE
            {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} = {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])}
            AND {self._json(tbl='l', col='l_commitdate', dt=dts['l_commitdate'])} < {self._json(tbl='l', col='l_receiptdate', dt=dts['l_receiptdate'])}
    )
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;

    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 4

        Returns
        -------
        list[str]
        """

        return [
            "o_orderpriority",
            "o_orderdate",
            "o_orderkey",
            "l_orderkey",
            "l_commitdate",
            "l_receiptdate"
        ]
