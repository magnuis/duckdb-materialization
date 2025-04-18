from queries.query import Query


class Q10(Query):
    """
    TPC-H Query 10
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 10, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])} AS c_custkey,
    {self._json(tbl='c', col='c_name', dt=dts['c_name'])} AS c_name,
    SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])})) AS revenue,
    {self._json(tbl='c', col='c_acctbal', dt=dts['c_acctbal'])} AS c_acctbal,
    {self._json(tbl='n', col='n_name', dt=dts['n_name'])} AS n_name,
    {self._json(tbl='c', col='c_address', dt=dts['c_address'])} AS c_address,
    {self._json(tbl='c', col='c_phone', dt=dts['c_phone'])} AS c_phone,
    {self._json(tbl='c', col='c_comment', dt=dts['c_comment'])} AS c_comment
FROM
    test_table c,
    test_table o,
    test_table l,
    test_table n
WHERE
    {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])} = {self._json(tbl='o', col='o_custkey', dt=dts['o_custkey'])}
    AND {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])} = {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])}
    AND {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} >= DATE '1993-10-01'
    AND {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} < DATE '1994-01-01'
    AND {self._json(tbl='l', col='l_returnflag', dt=dts['l_returnflag'])} = 'R'
    AND {self._json(tbl='c', col='c_nationkey', dt=dts['c_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
GROUP BY
    {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])},
    {self._json(tbl='c', col='c_name', dt=dts['c_name'])},
    {self._json(tbl='c', col='c_acctbal', dt=dts['c_acctbal'])},
    {self._json(tbl='c', col='c_phone', dt=dts['c_phone'])},
    {self._json(tbl='n', col='n_name', dt=dts['n_name'])},
    {self._json(tbl='c', col='c_address', dt=dts['c_address'])},
    {self._json(tbl='c', col='c_comment', dt=dts['c_comment'])}
ORDER BY
    revenue DESC
LIMIT
    20;
    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 10

        Returns
        -------
        list[str]
        """

        return [
            "c_custkey",
            "c_name",
            "l_extendedprice",
            "l_discount",
            "c_acctbal",
            "n_name",
            "c_address",
            "c_phone",
            "c_comment",
            "o_custkey",
            "o_orderdate",
            "o_orderkey",
            "l_orderkey",
            "l_returnflag",
            "c_nationkey",
            "n_nationkey"
        ]
