from queries.query import Query


class Q21(Query):
    """
    TPC-H Query 21
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 21, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='s', col='s_name', dt=dts['s_name'])} AS s_name,
    COUNT(*) AS numwait
FROM
    test_table s,
    test_table l1,
    test_table o,
    test_table n
WHERE
    {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = {self._json(tbl='l1', col='l_suppkey', dt=dts['l_suppkey'])}
    AND {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])} = {self._json(tbl='l1', col='l_orderkey', dt=dts['l_orderkey'])}
    AND {self._json(tbl='o', col='o_orderstatus', dt=dts['o_orderstatus'])} = 'F'
    AND {self._json(tbl='l1', col='l_receiptdate', dt=dts['l_receiptdate'])} > {self._json(tbl='l1', col='l_commitdate', dt=dts['l_commitdate'])}
    AND EXISTS (
        SELECT
            *
        FROM
            test_table l2
        WHERE
            {self._json(tbl='l2', col='l_orderkey', dt=dts['l_orderkey'])} = {self._json(tbl='l1', col='l_orderkey', dt=dts['l_orderkey'])}
            AND {self._json(tbl='l2', col='l_suppkey', dt=dts['l_suppkey'])} <> {self._json(tbl='l1', col='l_suppkey', dt=dts['l_suppkey'])}
    )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            test_table l3
        WHERE
            {self._json(tbl='l3', col='l_orderkey', dt=dts['l_orderkey'])} = {self._json(tbl='l1', col='l_orderkey', dt=dts['l_orderkey'])}
            AND {self._json(tbl='l3', col='l_suppkey', dt=dts['l_suppkey'])} <> {self._json(tbl='l1', col='l_suppkey', dt=dts['l_suppkey'])}
            AND {self._json(tbl='l3', col='l_receiptdate', dt=dts['l_receiptdate'])} > {self._json(tbl='l3', col='l_commitdate', dt=dts['l_commitdate'])}
    )
    AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
    AND {self._json(tbl='n', col='n_name', dt=dts['n_name'])} = 'SAUDI ARABIA'
GROUP BY
    {self._json(tbl='s', col='s_name', dt=dts['s_name'])}
ORDER BY
    numwait DESC,
    {self._json(tbl='s', col='s_name', dt=dts['s_name'])}
LIMIT
    100;

    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 21

        Returns
        -------
        list[str]
        """

        return [
            "s_name",
            "s_suppkey",
            "s_nationkey",
            "l_suppkey",
            "l_orderkey",
            "l_receiptdate",
            "l_commitdate",
            "o_orderkey",
            "o_orderstatus",
            "n_nationkey",
            "n_name"
        ]
