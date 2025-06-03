from queries.query import Query


class Q14(Query):
    """
    TPC-H Query 14
    """

    def __init__(self):
        super().__init__()

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

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 1

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """
        Get the underlying column names used in the query along with their position 
        in the query (e.g., SELECT, WHERE, GROUP BY, ORDER BY clauses).

        Returns
        -------
        dict
            A dictionary with the following keys:
            - 'select': list of underlying column names used in the SELECT clause.
            - 'where': list of underlying column names used in the WHERE clause that are not joins.
            - 'group_by': list of underlying column names used in the GROUP BY clause.
            - 'order_by': list of underlying column names used in the ORDER BY clause.
            - 'join': list of underlying column names used in a join operation (including WHERE)
        """
        return {
            'select': [
                "p_type",
                "l_extendedprice",
                "l_discount"
            ],
            'where': [
                "l_shipdate"
            ],
            'group_by': [],
            'order_by': [],
            'join': {
                "l_partkey": ["p_partkey"],
                "p_partkey": ["l_partkey"],
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "l_partkey": True,
            "p_partkey": False,
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "l_shipdate": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "l_partkey": 0,
            "p_partkey": 1,
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
