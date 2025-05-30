from queries.query import Query


class Q6(Query):
    """
    Twitter Query 6
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 6, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""

SELECT 
    {self._json(col='user_screenName', tbl='user_info', dt=dts['user_screenName'])} AS screen_name,
    SUM({self._json(col='user_followersCount', tbl='user_info', dt=dts['user_followersCount'])}) AS followers_count,
    COUNT(DISTINCT reply.id_str) AS reply_count,
    COUNT(DISTINCT retweet.id_str) AS retweet_count
FROM 
    test_table user_info,
    (
        SELECT 
            {self._json(col='id_str', tbl='test_table', dt=dts['id_str'])} AS id_str,
            {self._json(col='retweetedStatus_user_idStr', tbl='test_table', dt=dts['retweetedStatus_user_idStr'])} AS retweeter
        FROM test_table 
    ) AS retweet,
    (
        SELECT 
            {self._json(col='id_str', tbl='test_table', dt=dts['id_str'])} AS id_str,
            {self._json(col='inReplyToUserIdStr', tbl='test_table', dt=dts['inReplyToUserIdStr'])} AS reply_to_user
        FROM test_table
    ) AS reply
WHERE 
    {self._json(col='user_followersCount', tbl='user_info', dt=dts['user_followersCount'])} > 1000
    AND reply.reply_to_user = {self._json(col='user_idStr', tbl='user_info', dt=dts['user_idStr'])}
    AND retweet.retweeter =  {self._json(col='user_idStr', tbl='user_info', dt=dts['user_idStr'])}
GROUP BY 
     {self._json(col='user_idStr', tbl='user_info', dt=dts['user_idStr'])}
ORDER BY 
    followers_count DESC, 
    (reply_count + retweet_count) DESC
LIMIT 15;
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 2

    # TODO
    def columns_used_with_position(self,) -> dict[str, list[str]]:
        """
        Get the columns used in Twitter Query 6 along with their position in the query 
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
                'user_screenName',
                'user_followersCount',
                'id_str',
                'retweetedStatus_user_idStr',
                'id_str',
                'inReplyToUserIdStr'
            ],
            'where': [
                "user_followersCount"
            ],
            'group_by': [
                'user_idStr'
            ],
            'order_by': [
            ],
            'join': {
                "user_idStr": [None, None]
            }
        }

    # TODO
    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            'user_followersCount': 1
        }

        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            'retweetedStatus_idStr': 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
