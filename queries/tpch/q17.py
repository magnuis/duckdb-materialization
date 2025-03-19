from queries.query import Query


class Q17(Query):
    """
    TPC-H Query 17
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 17, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])}) / 7.0 AS avg_yearly
FROM
    test_table l,
    test_table p
WHERE
    {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
    AND {self._json(tbl='p', col='p_brand', dt=dts['p_brand'])} = 'Brand#23'
    AND {self._json(tbl='p', col='p_container', dt=dts['p_container'])} = 'MED BOX'
    AND {self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])} < (
        SELECT
            0.2 * AVG({self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])})
        FROM
            test_table l
        WHERE
            {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
    );

    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 17

        Returns
        -------
        list[str]
        """

        return [
            "l_extendedprice",
            "l_partkey",
            "l_quantity",
            "p_partkey",
            "p_brand",
            "p_container"
        ]
