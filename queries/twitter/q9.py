from queries.query import Query


class Q9(Query):
    """
    Twitter Query 9
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 9, adjusted to current db materialization

        Returns
        -------
        str
        """
        dts = self._get_field_types(fields=fields)
        acs = self._get_field_accesses(fields=fields)

        return f"""
        SELECT
            {self._json(col='idStr', tbl='orig', dt=dts['idStr'], acs=acs['idStr'])}                                    AS original_tweet_id,
            {self._json(col='user_screenName', tbl='orig', dt=dts['user_screenName'], acs=acs['user_screenName'])}                           AS original_user,
            {self._json(col='user_followersCount', tbl='orig', dt=dts['user_followersCount'], acs=acs['user_followersCount'])}                            AS original_user_followers,
            COUNT(DISTINCT {self._json(col='user_screenName', tbl='rt', dt=dts['user_screenName'], acs=acs['user_screenName'])})           AS num_distinct_retweeters,
            COALESCE(SUM(TRY_CAST({self._json(col='user_followersCount', tbl='rt', dt=dts['user_followersCount'], acs=acs['user_followersCount'])} AS INT)), 0)          AS total_followers_of_retweeters,
            COALESCE(MAX(TRY_CAST({self._json(col='retweetedStatus_retweetCount', tbl='rt', dt=dts['retweetedStatus_retweetCount'], acs=acs['retweetedStatus_retweetCount'])} AS INT)), 0)            AS max_retweetCount_among_retweets,
            COUNT(DISTINCT {self._json(col='idStr', tbl='rep', dt=dts['idStr'], acs=acs['idStr'])})                   AS num_distinct_replies,
            ROUND(
                100.0
                * COUNT(DISTINCT {self._json(col='user_screenName', tbl='rt', dt=dts['user_screenName'], acs=acs['user_screenName'])})
                / NULLIF(
                    (
                        SELECT COUNT(DISTINCT {self._json(col='user_screenName', tbl='sub', dt=dts['user_screenName'], acs=acs['user_screenName'])})
                        FROM test_table AS sub
                    ),
                    0
                ),
                2
            )                                                AS pct_of_all_users_who_retweeted
        FROM
            test_table AS orig
            LEFT JOIN test_table AS rt
                ON {self._json(col='retweetedStatus_idStr', tbl='rt', dt=dts['retweetedStatus_idStr'], acs=acs['retweetedStatus_idStr'])} = {self._json(col='idStr', tbl='orig', dt=dts['idStr'], acs=acs['idStr'])}
            LEFT JOIN test_table AS rep
                ON {self._json(col='inReplyToUserIdStr', tbl='rep', dt=dts['inReplyToUserIdStr'], acs=acs['inReplyToUserIdStr'])} = {self._json(col='user_screenName', tbl='orig', dt=dts['user_screenName'], acs=acs['user_screenName'])}
        WHERE
            {self._json(col='retweetedStatus_idStr', tbl='orig', dt=dts['retweetedStatus_idStr'], acs=acs['retweetedStatus_idStr'])} IS NULL
            AND {self._json(col='inReplyToUserIdStr', tbl='orig', dt=dts['inReplyToUserIdStr'], acs=acs['inReplyToUserIdStr'])} IS NULL
        GROUP BY
            {self._json(col='idStr', tbl='orig', dt=dts['idStr'], acs=acs['idStr'])},
            {self._json(col='user_screenName', tbl='orig', dt=dts['user_screenName'], acs=acs['user_screenName'])},
            {self._json(col='user_followersCount', tbl='orig', dt=dts['user_followersCount'], acs=acs['user_followersCount'])}
        ORDER BY
            num_distinct_retweeters DESC
        LIMIT 50;
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 2

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """
        Get the columns used in Twitter Query 9 along with their position in the query 
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
                'idStr',
                'user_screenName',
                'user_followersCount',
                'user_screenName',
                'user_followersCount',
                'retweetedStatus_retweetCount',
                'idStr',
                'user_screenName'
            ],
            'where': [
                'retweetedStatus_idStr',
                'inReplyToUserIdStr'
            ],
            'group_by': [
                'idStr',
                'user_screenName',
                'user_followersCount'
            ],
            'order_by': [
                'num_distinct_retweeters'
            ],
            'join': {
                'retweetedStatus_idStr': ['idStr'],
                'inReplyToUserIdStr': ['user_screenName'],
                'idStr': ['retweetedStatus_idStr'],
                'user_screenName': ['inReplyToUserIdStr']
            }
        }

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query-specific implementation: number of times the WHERE field
        can be applied directly if materialized into a regular column.
        """
        field_map = {
            'retweetedStatus_idStr': 1,
            'inReplyToUserIdStr': 1

        }
        return field_map.get(field, None)

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query-specific implementation: number of times the field appears in
        a join (including WHERE t1.f1 = t2.f2) AND there are no other
        predicates on that table.
        """
        field_map = {
            'retweetedStatus_idStr': 1,
            'inReplyToUserIdStr': 1,
            'idStr': 0,
            'user_screenName': 0
        }
        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")
        return field_map[field]
