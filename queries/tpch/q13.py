from queries.query import Query


class Q13(Query):
    """
    TPC-H Query 13
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 13, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])} AS c_custkey,
            COUNT({self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])}) AS c_count
        FROM
            test_table c LEFT OUTER JOIN test_table o ON
                {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])} = {self._json(tbl='o', col='o_custkey', dt=dts['o_custkey'])}
                AND {self._json(tbl='o', col='o_comment', dt=dts['o_comment'])} NOT LIKE '%special%requests%'
        GROUP BY
            {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])}
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;
    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 13

        Returns
        -------
        list[str]
        """

        return [
            "c_custkey",
            "o_custkey",
            "o_orderkey",
            "o_comment"
        ]
