from queries.query import Query


class Q1(Query):
    """
    TPC-H Query 1
    """

    def __init__(self):
        super().__init__()

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 1, adjusted to current db materializaiton

        Returns
        -------
        str
        """

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

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "l_shipdate": 1
        }

        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {}

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
