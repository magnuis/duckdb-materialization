from queries.query import Query


class Q3(Query):
    """
    TPC-H Query 3
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 3, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} AS s_acctbal,
    SUM( {self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])})) AS revenue,
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} AS o_orderdate,
    {self._json(tbl='o', col='o_shippriority', dt=dts['o_shippriority'])} AS o_shippriority,
FROM
    test_table c,
    test_table o,
    test_table l
WHERE
    {self._json(tbl='c', col='c_mktsegment', dt=dts['c_mktsegment'])} = 'BUILDING'
    AND {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])} = {self._json(tbl='o', col='o_custkey', dt=dts['o_custkey'])}
    AND {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} = {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])}
    AND {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} < DATE '1995-03-15'
    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} > DATE '1995-03-15'
GROUP BY
    {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])},
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])},
    {self._json(tbl='o', col='o_shippriority', dt=dts['o_shippriority'])}
ORDER BY
    revenue DESC,
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])}
LIMIT
    10;
    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 3

        Returns
        -------
        list[str]
        """

        return [
            "l_orderkey",
            "l_extendedprice",
            "l_discount",
            "o_orderdate",
            "o_shippriority",
            "c_mktsegment",
            "c_custkey",
            "o_custkey",
            "o_orderkey",
            "l_shipdate"
        ]
