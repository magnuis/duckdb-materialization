from queries.query import Query


class Q11(Query):
    """
    Twitter Query 11
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 11, adjusted to current db materialization

        Returns
        -------
        str
        """

        return f"""
        SELECT
            {self._json(col='user_idStr', tbl='u', fields=fields)}       AS user_ider_id,
            {self._json(col='user_screenName', tbl='u', fields=fields)}  AS screen_name,
            {self._json(col='delete_timestampMs', tbl='d', fields=fields)}                     AS delete_timestamp
        FROM
            test_table AS u,
            test_table AS d
        WHERE
            {self._json(col='source', tbl='u', fields=fields)} LIKE '%Twitter for iPhone%'
            AND {self._json(col='delete_status_userIdStr', tbl='d', fields=fields)} = {self._json(col='user_idStr', tbl='u', fields=fields)};
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 0

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """
        Get the columns used in Twitter Query 11 along with their position in the query 
        (e.g., SELECT, WHERE, GROUP BY, ORDER BY clauses).

        Returns
        -------
        dict
            A dictionary with the following keys:
            - 'select': list of column names used in the SELECT clause (one entry per appearance).
            - 'where': list of column names used in the WHERE clause (one entry per appearance).
            - 'group_by': list of column names used in the GROUP BY clause.
            - 'order_by': list of column or alias names used in the ORDER BY clause (one entry per appearance).
            - 'join': mapping of join fields to the fields they're equated against.
        """
        return {
            'select': [
                'user_idStr',
                'user_screenName',
                'delete_timestampMs'
            ],
            'where': [
                'source'
            ],
            'group_by': [],
            'order_by': [],
            'join': {
                'delete_status_userIdStr': ['user_idStr'],
                'user_idStr': ['delete_status_userIdStr']
            }
        }

    def get_field_weight(self, field: str, prev_materialization: list[str]) -> int:

        field_map = {
            'user_idStr': 2*self.poor_field_weight,
            "user_screenName": 1*self.poor_field_weight,
            "delete_timestampMs": 1*self.poor_field_weight,
            "source": 1*self.good_field_weight,
            "delete_status_userIdStr": 1*self.good_field_weight
        }
        if field not in field_map:
            raise ValueError(f"{field} not a query field")

        return field_map.get(field, 0)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query-specific implementation: number of times the WHERE field
        can be applied directly if materialized into a regular column.
        """
        field_map = {
            'source': 1
        }
        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query-specific implementation: number of times the field appears in
        a join (including WHERE t1.f1 = t2.f2) AND there are no other
        predicates on that table.
        """
        field_map = {
            'delete_status_userIdStr': 1,
            'user_idStr': 0
        }
        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")
        return field_map[field]
