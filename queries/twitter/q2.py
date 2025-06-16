from queries.query import Query


class Q2(Query):
    """
    Twitter Query 2
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 2, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
            SELECT 
                {self._json(col='source', tbl='t', fields=fields)},
                COUNT(*) AS tweet_count
            FROM test_table t
            GROUP BY {self._json(col='source', tbl='t', fields=fields)}
            ORDER BY tweet_count, {self._json(col='source', tbl='t', fields=fields)} DESC;
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 0

    # TODO
    def columns_used_with_position(self,) -> dict[str, list[str]]:
        """
        Get the columns used in Twitter Query 2 along with their position in the query 
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
                'source'
            ],
            'where': [
            ],
            'group_by': [
                'source'
            ],
            'order_by': [
                'source'
            ],
            'join': {

            }
        }

    def get_field_weight(self, field: str, prev_materialization: list[str]) -> int:
        field_map = {
            'source': 1*self.good_field_weight + 1*self.poor_field_weight
        }
        if field not in field_map:
            raise ValueError(f"{field} not a query field")
        return field_map.get(field, 0)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {}

        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {}

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
