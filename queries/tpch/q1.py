from queries.query import Query


class Q1(Query):
    """
    TPC-H Query 1
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "l": ["l_returnflag",
                  "l_linestatus",
                  "l_quantity",
                  "l_extendedprice",
                  "l_discount",
                  "l_tax",
                  "l_shipdate"
                  ]
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 1, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
    SELECT
        {self._json(tbl='l', col='l_returnflag', dts=dts)} AS l_returnflag,
        {self._json(tbl='l', col='l_linestatus', dts=dts)} AS l_linestatus,
        SUM({self._json(tbl='l', col='l_quantity', dts=dts)}) AS sum_qty,
        SUM({self._json(tbl='l', col='l_extendedprice', dts=dts)}) AS sum_base_price,
        SUM({self._json(tbl='l', col='l_extendedprice', dts=dts)} * (1 - {self._json(tbl='l', col='l_discount', dts=dts)})) AS sum_disc_price,
        SUM({self._json(tbl='l', col='l_extendedprice', dts=dts)} * (1 - {self._json(tbl='l', col='l_discount', dts=dts)}) * (1 + {self._json(tbl='l', col='l_tax', dts=dts)})) AS sum_charge,
        AVG({self._json(tbl='l', col='l_quantity', dts=dts)}) AS avg_qty,
        AVG({self._json(tbl='l', col='l_extendedprice', dts=dts)}) AS avg_price,
        AVG({self._json(tbl='l', col='l_discount', dts=dts)}) AS avg_disc,
        COUNT(*) AS count_order
    FROM
        extracted l
    WHERE
        {self._json(tbl='l', col='l_shipdate', dts=dts)} <= DATE '1998-12-01' - INTERVAL '90' DAY
    GROUP BY
        l_returnflag,
        l_linestatus
    ORDER BY
        l_returnflag,
        l_linestatus;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 0

    def columns_used_with_position(self,) -> dict[str, list[str]]:
        """
        Get the columns used in TPC-H Query 1 along with their position in the query 
        (e.g., SELECT, WHERE, GROUP BY, ORDER BY clauses).

        Returns
        -------
        dict
            A dictionary with the following keys:
            - 'select': list of column names used in the SELECT clause.
            - 'where': list of column names used in the WHERE clause that are not joins.
            - 'group_by': list of column names used in the GROUP BY clause.
            - 'order_by': list of column names used in the ORDER BY clause.
            - 'join': list of column names used in a join operation (including WHERE).
        """
        return {
            'select': [
                "l_returnflag",
                "l_linestatus",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_tax"
            ],
            'where': [
                "l_shipdate"
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {}
        }
