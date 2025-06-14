from queries.query import Query


class Q5(Query):
    """
    Twitter Query 5
    """

    def __init__(self):
        super().__init__()

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 5, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_types(fields=fields)
        acs = self._get_field_accesses(fields=fields)

        return f"""
            SELECT ROUND(
                100.0 * COUNT(DISTINCT {self._json(col='user_idStr', tbl='test_table', dt=dts['user_idStr'], acs=acs['user_idStr'])}) /
                (SELECT COUNT(DISTINCT {self._json(col='user_idStr', tbl='test_table', dt=dts['user_idStr'], acs=acs['user_idStr'])}) FROM test_table), 2) AS percentage
            FROM test_table
            WHERE lower({self._json(col='text', tbl='test_table', dt=dts['text'], acs=acs['text'])}) LIKE '%covid-19%';
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 0

    # TODO
    def columns_used_with_position(self,) -> dict[str, list[str]]:
        """
        Get the columns used in Twitter Query 5 along with their position in the query 
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
                'user_idStr',
                'user_idStr'
            ],
            'where': [
                "text"
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {

            }
        }

    def get_field_weight(self, field: str, prev_materialization: list[str]) -> int:
        field_map = {
            'text': 1*self.GOOD_FIELD_WEIGHT,
            'user_idStr':  2*self.POOR_FIELD_WEIGHT
        }
        if field not in field_map:
            raise ValueError(f"{field} not a query field")

        return field_map.get(field, 0)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            'text': 0
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
