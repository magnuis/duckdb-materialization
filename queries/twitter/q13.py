from queries.query import Query


class Q13(Query):
    """
    Twitter Query 13
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 13, adjusted to current db materialization

        Returns
        -------
        str
        """

        return f"""
        SELECT
            {self._json(col='delete_status_userIdStr', tbl='d1', fields=fields)} AS deleted_user_id,
            (MAX(TRY_CAST({self._json(col='delete_timestampMs', tbl='d1', fields=fields)} AS BIGINT))
             - MIN(TRY_CAST({self._json(col='delete_timestampMs', tbl='d1', fields=fields)} AS BIGINT))) AS delete_time_diff_ms
        FROM
            test_table AS d1,
            test_table AS d2
        WHERE
            {self._json(col='delete_status_userIdStr', tbl='d1', fields=fields)}
                = {self._json(col='delete_status_userIdStr', tbl='d2', fields=fields)}
        GROUP BY
            {self._json(col='delete_status_userIdStr', tbl='d1', fields=fields)}
        HAVING
            delete_time_diff_ms > 0
        ORDER BY
            delete_time_diff_ms DESC;
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 0

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """
        Get the columns used in Twitter Query 13 along with their position in the query 
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
                'delete_status_userIdStr',
                'delete_timestampMs',
                'delete_timestampMs'
            ],
            'where': [
            ],
            'group_by': [
                'delete_status_userIdStr'
            ],
            'order_by': [
            ],
            'join': {
                'delete_status_userIdStr': ['delete_status_userIdStr']
            }
        }

    def get_field_weight(self, field: str, prev_materialization: list[str]) -> int:

        field_map = {
            "delete_status_userIdStr": 2*self.good_field_weight + 2*self.poor_field_weight,
            "delete_timestampMs": 1*self.poor_field_weight

        }
        if field not in field_map:
            raise ValueError(f"{field} not a query field")

        return field_map.get(field, 0)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query-specific implementation: number of times the WHERE field
        can be applied directly if materialized into a regular column.
        """
        raise ValueError(f"{field} not a WHERE field")

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query-specific implementation: number of times the field appears in
        a join (including WHERE t1.f1 = t2.f2) AND there are no other
        predicates on that table.
        """
        field_map = {
            'delete_status_userIdStr': 2
        }
        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")
        return field_map[field]
