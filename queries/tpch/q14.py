from queries.query import Query


class Q14(Query):
    """
    TPC-H Query 14
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 14, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    100.00 * SUM(
        CASE
            WHEN {self._json(tbl='p', col='p_type', dt=dts['p_type'])} LIKE 'PROMO%' THEN {self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])})
            ELSE 0
        END
    ) / SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])})) AS promo_revenue
FROM
    test_table l,
    test_table p
WHERE
    {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])} = {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} 
    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} >= DATE '1995-09-01'
    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} < DATE '1995-10-01';

    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 14

        Returns
        -------
        list[str]
        """

        return [
            "l_extendedprice",
            "l_discount",
            "p_type",
            "l_partkey",
            "p_partkey",
            "l_shipdate"
        ]
