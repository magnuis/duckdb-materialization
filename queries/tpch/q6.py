from queries.query import Query


class Q6(Query):
    """
    TPC-H Query 6
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 6, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    SUM({self._json(tbl='l', col='l_extendedprice', fields=fields)} * (1 - {self._json(tbl='l', col='l_discount', fields=fields)})) AS revenue
FROM
    test_table l
WHERE
    {self._json(tbl='l', col='l_shipdate', fields=fields)} >= DATE '1994-01-01'
    AND {self._json(tbl='l', col='l_shipdate', fields=fields)} < DATE '1995-01-01'
    AND {self._json(tbl='l', col='l_discount', fields=fields)} BETWEEN 0.05 AND 0.07
    AND {self._json(tbl='l', col='l_quantity', fields=fields)} < 24;

    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """

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
            - 'join': list of column names used in a join operation (including WHERE)
        """
        return {
            'select': [
                "l_extendedprice", "l_discount",
            ],
            'where': [
                "l_shipdate",
                "l_discount",
                "l_quantity"
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {}
        }

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "l_shipdate": 1,
            "l_discount": 1,
            "l_quantity": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """

        field_map = {}

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
