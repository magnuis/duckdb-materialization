from queries.query import Query


class Q11(Query):
    """
    TPC-H Query 11
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 11, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])} AS ps_partkey,
    SUM({self._json(tbl='ps', col='ps_supplycost', dt=dts['ps_supplycost'])} * {self._json(tbl='ps', col='ps_availqty', dt=dts['ps_availqty'])}) AS value
FROM
    test_table ps,
    test_table s,
    test_table n
WHERE
    {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])} = {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])}
    AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
    AND {self._json(tbl='n', col='n_name', dt=dts['n_name'])} = 'GERMANY'
GROUP BY
    {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])}
HAVING
    SUM({self._json(tbl='ps', col='ps_supplycost', dt=dts['ps_supplycost'])} * {self._json(tbl='ps', col='ps_availqty', dt=dts['ps_availqty'])}) > (
        SELECT
            SUM({self._json(tbl='ps', col='ps_supplycost', dt=dts['ps_supplycost'])} * {self._json(tbl='ps', col='ps_availqty', dt=dts['ps_availqty'])}) * 0.0001
        FROM
            test_table ps,
            test_table s,
            test_table n
        WHERE
            {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])} = {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])}
            AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
            AND {self._json(tbl='n', col='n_name', dt=dts['n_name'])} = 'GERMANY'
    )
ORDER BY
    value DESC;

    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 11

        Returns
        -------
        list[str]
        """

        return [
            "ps_partkey",
            "ps_supplycost",
            "ps_availqty",
            "ps_suppkey",
            "s_suppkey",
            "s_nationkey",
            "n_nationkey",
            "n_name"
        ]
