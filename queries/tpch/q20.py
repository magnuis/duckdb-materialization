from queries.query import Query


class Q20(Query):
    """
    TPC-H Query 20
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 20, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='s', col='s_name', dt=dts['s_name'])} AS s_name,
    {self._json(tbl='s', col='s_address', dt=dts['s_address'])} AS s_address
FROM
    test_table s,
    test_table n
WHERE
    {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} IN (
        SELECT
            {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])}
        FROM
            test_table ps
        WHERE
            {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])} IN (
                SELECT
                    {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])}
                FROM
                    test_table p
                WHERE
                    {self._json(tbl='p', col='p_name', dt=dts['p_name'])} LIKE 'forest%'
            )
            AND {self._json(tbl='ps', col='ps_availqty', dt=dts['ps_availqty'])} > (
                SELECT
                    0.5 * SUM({self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])})
                FROM
                    test_table l
                WHERE
                    {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])} = {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])}
                    AND {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])} = {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])}
                    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} >= DATE '1994-01-01'
                    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} < DATE '1995-01-01'
            )
    )
    AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
    AND {self._json(tbl='n', col='n_name', dt=dts['n_name'])} = 'CANADA'
ORDER BY
    {self._json(tbl='s', col='s_name', dt=dts['s_name'])};
    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 20

        Returns
        -------
        list[str]
        """

        return [
            "s_name",
            "s_address",
            "s_suppkey",
            "s_nationkey",
            "n_nationkey",
            "n_name",
            "ps_suppkey",
            "ps_partkey",
            "ps_availqty",
            "p_partkey",
            "p_name",
            "l_quantity",
            "l_partkey",
            "l_suppkey",
            "l_shipdate"
        ]
