from queries.query import Query


class Q13(Query):
    """
    Twitter Query 13
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 13, adjusted to current db materialization

        Returns
        -------
        str
        """
        dts = self._get_field_types(fields=fields)
        acs = self._get_field_accesses(fields=fields)

        return f"""
        SELECT
            {self._json(col='idStr', tbl='orig', dt=dts['idStr'], acs=acs['idStr'])}                                               AS original_tweet_id,
            COUNT(DISTINCT {self._json(col='idStr', tbl='rt', dt=dts['idStr'], acs=acs['idStr'])})                                   AS num_retweets,
            COUNT(DISTINCT {self._json(col='idStr', tbl='rep', dt=dts['idStr'], acs=acs['idStr'])})                                  AS num_replies
        FROM
            test_table AS orig
            LEFT JOIN test_table AS rt
                ON {self._json(col='retweetedStatus_idStr', tbl='rt', dt=dts['retweetedStatus_idStr'], acs=acs['retweetedStatus_idStr'])} = {self._json(col='idStr', tbl='orig', dt=dts['idStr'], acs=acs['idStr'])}
                AND LOWER({self._json(col='text', tbl='rt', dt=dts['text'], acs=acs['text'])}) LIKE '%bad%'
            LEFT JOIN test_table AS rep
                ON {self._json(col='inReplyToUserIdStr', tbl='rep', dt=dts['inReplyToUserIdStr'], acs=acs['inReplyToUserIdStr'])} = {self._json(col='idStr', tbl='orig', dt=dts['idStr'], acs=acs['idStr'])}
        WHERE
            {self._json(col='lang', tbl='orig', dt=dts['lang'], acs=acs['lang'])} = 'en'
            AND TRY_CAST({self._json(col='user_followersCount', tbl='orig', dt=dts['user_followersCount'], acs=acs['user_followersCount'])} AS INT) > 75
        GROUP BY
            {self._json(col='idStr', tbl='orig', dt=dts['idStr'], acs=acs['idStr'])}
        ORDER BY
            num_retweets DESC,
            num_replies DESC
        LIMIT 25;
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 2

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
                'idStr',
                'idStr',
                'idStr'
            ],
            'where': [
                'lang',
                'user_followersCount'
            ],
            'group_by': [
                'idStr'
            ],
            'order_by': [
                'num_retweets',
                'num_replies'
            ],
            'join': {
                'retweetedStatus_idStr': ['idStr'],
                'inReplyToUserIdStr': ['idStr'],
                'idStr': ['retweetedStatus_idStr', 'inReplyToUserIdStr']
            }
        }

    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query-specific implementation: number of times the WHERE field
        can be applied directly if materialized into a regular column.
        """
        if field == 'lang' and 'user_followersCount' in prev_materialization:
            return 0
        if field == 'user_followersCount' and 'lang' in prev_materialization:
            return 0

        field_map = {
            'lang': 1,
            'user_followersCount': 1,
            'text': 0
        }
        return field_map.get(field, None)

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query-specific implementation: number of times the field appears in
        a join (including WHERE t1.f1 = t2.f2) AND there are no other
        predicates on that table.
        """
        field_map = {
            'retweetedStatus_idStr': 0,
            'inReplyToUserIdStr': 1,
            'idStr': 0
        }
        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")
        return field_map[field]
