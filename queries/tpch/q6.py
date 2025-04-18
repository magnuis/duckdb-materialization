from queries.query import Query


class Q6(Query):
    """
    TPC-H Query 6
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 6, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])})) AS revenue
FROM
    test_table l
WHERE
    {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} >= DATE '1994-01-01'
    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} < DATE '1995-01-01'
    AND {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])} BETWEEN 0.05 AND 0.07
    AND {self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])} < 24;

    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 6

        Returns
        -------
        list[str]
        """

        return [
            "l_extendedprice",
            "l_discount",
            "l_shipdate",
            "l_quantity"
        ]
