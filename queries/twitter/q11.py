from queries.query import Query


class Q11(Query):
    """
    Twitter Query 11
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 11, adjusted to current db materialization

        Returns
        -------
        str
        """
        dts = self._get_field_types(fields=fields)
        acs = self._get_field_accesses(fields=fields)

        return f"""
        SELECT
            {self._json(col='user_idStr', tbl='u', dt=dts['user_idStr'], acs=acs['user_idStr'])}       AS user_ider_id,
            {self._json(col='user_screenName', tbl='u', dt=dts['user_screenName'], acs=acs['user_screenName'])}  AS screen_name,
            {self._json(col='delete_status_idStr', tbl='d', dt=dts['delete_status_idStr'], acs=acs['delete_status_idStr'])}       AS deleted_status_id,
            {self._json(col='delete_timestampMs', tbl='d', dt=dts['delete_timestampMs'], acs=acs['delete_timestampMs'])}                     AS delete_timestamp
        FROM
            test_table AS u,
            test_table AS d
        WHERE
            {self._json(col='source', tbl='u', dt=dts['source'], acs=acs['source'])} LIKE '%Twitter for iPhone%'
            AND {self._json(col='delete_status_userIdStr', tbl='d', dt=dts['delete_status_userIdStr'], acs=acs['delete_status_userIdStr'])} = {self._json(col='user_idStr', tbl='u', dt=dts['user_idStr'], acs=acs['user_idStr'])};
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
                'delete_status_idStr',
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

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query-specific implementation: number of times the WHERE field
        can be applied directly if materialized into a regular column.
        """
        field_map = {
            'source': 1
        }
        return field_map.get(field, None)

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
