from queries.query import Query


class Q18(Query):
    """
    TPC-H Query 18
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 18, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='c', col='c_name', dt=dts['c_name'])} AS c_name,
    {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])} AS c_custkey,
    {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])} AS o_orderkey,
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])} AS o_orderdate,
    {self._json(tbl='o', col='o_totalprice', dt=dts['o_totalprice'])} AS o_totalprice,
    SUM({self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])}) AS total_quantity
FROM
    test_table c,
    test_table o,
    test_table l
WHERE
    {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])} IN (
        SELECT
            {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])}
        FROM
            test_table l
        GROUP BY
            {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])}
        HAVING
            SUM({self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])}) > 300
    )
    AND {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])} = {self._json(tbl='o', col='o_custkey', dt=dts['o_custkey'])}
    AND {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])} = {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])}
GROUP BY
    {self._json(tbl='c', col='c_name', dt=dts['c_name'])},
    {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])},
    {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])},
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])},
    {self._json(tbl='o', col='o_totalprice', dt=dts['o_totalprice'])}
ORDER BY
    {self._json(tbl='o', col='o_totalprice', dt=dts['o_totalprice'])} DESC,
    {self._json(tbl='o', col='o_orderdate', dt=dts['o_orderdate'])}
LIMIT
    100;
    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 18

        Returns
        -------
        list[str]
        """

        return [
            "c_name",
            "c_custkey",
            "o_orderkey",
            "o_orderdate",
            "o_totalprice",
            "o_custkey",
            "l_quantity",
            "l_orderkey"
        ]
