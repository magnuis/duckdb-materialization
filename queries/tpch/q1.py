from queries.query import Query


class Q1(Query):
    """
    TPC-H Query 1
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 1, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        # TODO use more performant e.g. dict for loopup
        dts = self._get_field_accesses(fields=fields)

        return f"""
    SELECT
        {self._json(tbl='l', col='l_returnflag', dt=dts['l_returnflag'])} AS l_returnflag,
        {self._json(tbl='l', col='l_linestatus', dt=dts['l_linestatus'])} AS l_linestatus,
        SUM({self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])}) AS sum_qty,
        SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])}) AS sum_base_price,
        SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])})) AS sum_disc_price,
        SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])}) * (1 + {self._json(tbl='l', col='l_tax', dt=dts['l_tax'])})) AS sum_charge,
        AVG({self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])}) AS avg_qty,
        AVG({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])}) AS avg_price,
        AVG({self._json(tbl='l', col='l_discount', dt=dts['l_discount'])}) AS avg_disc,
        COUNT(*) AS count_order
    FROM
        test_table l
    WHERE
        {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} <= DATE '1998-12-01' - INTERVAL '90' DAY
    GROUP BY
        l_returnflag,
        l_linestatus
    ORDER BY
        l_returnflag,
        l_linestatus;
    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 1

        Returns
        -------
        list[str]
        """

        return [
            "l_returnflag",
            "l_linestatus",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_quantity",
            "l_shipdate"
        ]
