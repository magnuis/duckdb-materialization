from queries.query import Query


class Q8(Query):
    """
    Twitter Query 8
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 8, adjusted to current db materialization

        Returns
        -------
        str
        """

        return f"""
        SELECT
            ANY_VALUE({self._json(col='user_screenName', tbl='user_info', fields=fields)}) AS screen_name,
            SUM(TRY_CAST({self._json(col='user_followersCount', tbl='user_info', fields=fields)} AS INT)) AS followers_count,
            COUNT(DISTINCT {self._json(col='idStr', tbl='reply', fields=fields)}) AS reply_count,
            COUNT(DISTINCT {self._json(col='idStr', tbl='retweet', fields=fields)}) AS retweet_count
        FROM
            test_table AS user_info,
            test_table AS retweet,
            test_table AS reply
        WHERE
            TRY_CAST({self._json(col='user_followersCount', tbl='user_info', fields=fields)} AS INT) > 1000
            AND {self._json(col='inReplyToUserIdStr', tbl='reply', fields=fields)} = {self._json(col='user_idStr', tbl='user_info', fields=fields)}
            AND {self._json(col='retweetedStatus_user_idStr', tbl='retweet', fields=fields)} = {self._json(col='user_idStr', tbl='user_info', fields=fields)}
        GROUP BY
            {self._json(col='user_idStr', tbl='user_info', fields=fields)}
        ORDER BY
            followers_count DESC,
            (reply_count + retweet_count) DESC
        LIMIT 15;
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 0

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """
        Get the columns used in Twitter Query 8 along with their position in the query 
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
                'user_screenName',
                'user_followersCount',
                'idStr',
                'idStr'
            ],
            'where': [
                'user_followersCount'
            ],
            'group_by': [
                'user_idStr'
            ],
            'order_by': [
            ],
            'join': {
                'inReplyToUserIdStr': ['user_idStr'],
                'retweetedStatus_user_idStr': ['user_idStr'],
                'user_idStr': ['inReplyToUserIdStr', 'retweetedStatus_user_idStr']
            }
        }

    def get_field_weight(self, field: str, prev_materialization: list[str]) -> int:
        field_map = {
            'user_screenName': 1*self.poor_field_weight,
            "user_followersCount": 1 * self.good_field_weight + 1*self.poor_field_weight,
            "idStr": 2*self.poor_field_weight,
            'user_idStr':  3*self.poor_field_weight,
            "inReplyToUserIdStr": 1*self.good_field_weight,
            "retweetedStatus_user_idStr": 1*self.good_field_weight,
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
            'user_followersCount': 1
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
            'inReplyToUserIdStr': 1,
            'retweetedStatus_user_idStr': 1,
            'user_idStr': 0
        }
        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")
        return field_map[field]
