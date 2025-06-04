from queries.query import Query


class Q12(Query):
    """
    Twitter Query 12
    """

    def __init__(self):
        super().__init__()

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 12, adjusted to current db materialization

        Returns
        -------
        str
        """
        dts = self._get_field_types(fields=fields)
        acs = self._get_field_accesses(fields=fields)

        return f"""
        SELECT
            {self._json(col='user_screenName', tbl='orig', dt=dts['user_screenName'], acs=acs['user_screenName'])}                                   AS original_user,
            COUNT(DISTINCT {self._json(col='idStr', tbl='rt', dt=dts['idStr'], acs=acs['idStr'])})                                   AS total_retweet_rows,
            COUNT(DISTINCT {self._json(col='idStr', tbl='rep', dt=dts['idStr'], acs=acs['idStr'])})                                  AS total_reply_rows
        FROM
            test_table AS orig
            LEFT JOIN test_table AS rt
                ON {self._json(col='retweetedStatus_idStr', tbl='rt', dt=dts['retweetedStatus_idStr'], acs=acs['retweetedStatus_idStr'])} = {self._json(col='idStr', tbl='orig', dt=dts['idStr'], acs=acs['idStr'])}
            LEFT JOIN test_table AS rep
                ON {self._json(col='inReplyToUserIdStr', tbl='rep', dt=dts['inReplyToUserIdStr'], acs=acs['inReplyToUserIdStr'])} = {self._json(col='idStr', tbl='orig', dt=dts['idStr'], acs=acs['idStr'])}
        WHERE
            TRY_CAST({self._json(col='user_isTranslator', tbl='orig', dt=dts['user_isTranslator'], acs=acs['user_isTranslator'])} AS BOOLEAN)
            AND {self._json(col='inReplyToUserIdStr', tbl='orig', dt=dts['inReplyToUserIdStr'], acs=acs['inReplyToUserIdStr'])} IS NULL
        GROUP BY
            {self._json(col='user_screenName', tbl='orig', dt=dts['user_screenName'], acs=acs['user_screenName'])}
        ORDER BY
            total_retweet_rows DESC
        LIMIT 25;
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 2

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """
        Get the columns used in Twitter Query 12 along with their position in the query 
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
                'idStr',
                'idStr'
            ],
            'where': [
                'user_isTranslator',
                'inReplyToUserIdStr'
            ],
            'group_by': [
                'user_screenName'
            ],
            'order_by': [
            ],
            'join': {
                'retweetedStatus_idStr': ['idStr'],
                'inReplyToUserIdStr': ['idStr'],
                'idStr': ['retweetedStatus_idStr', 'inReplyToUserIdStr']
            }
        }

    def get_field_weight(self, field: str, prev_materialization: list[str]) -> int:
        user_isTranslator_weight = 1 * self.GOOD_FIELD_WEIGHT
        if field == 'user_isTranslator' and 'inReplyToUserIdStr' in prev_materialization:
            user_isTranslator_weight = 1 * self.POOR_FIELD_WEIGHT

        inReplyToUserIdStr_weight = 2 * self.GOOD_FIELD_WEIGHT
        if field == 'inReplyToUserIdStr' and 'user_isTranslator' in prev_materialization:
            inReplyToUserIdStr_weight = 1 * self.POOR_FIELD_WEIGHT + 1 * self.GOOD_FIELD_WEIGHT

        field_map = {
            "idStr": 4*self.POOR_FIELD_WEIGHT,
            'user_screenName': 2*self.POOR_FIELD_WEIGHT,
            "user_isTranslator": user_isTranslator_weight,
            "inReplyToUserIdStr": inReplyToUserIdStr_weight,
            "retweetedStatus_idStr": 1*self.GOOD_FIELD_WEIGHT,
        }
        if field not in field_map:
            raise ValueError(f"{field} not a query field")

        return field_map.get(field, 0)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query-specific implementation: number of times the WHERE field
        can be applied directly if materialized into a regular column.
        """
        if field == 'user_isTranslator' and 'inReplyToUserIdStr' in prev_materialization:
            return 0
        if field == 'inReplyToUserIdStr' and 'user_isTranslator' in prev_materialization:
            return 0

        field_map = {
            'user_isTranslator': 1,
            'inReplyToUserIdStr': 1
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
            'retweetedStatus_idStr': 1,
            'inReplyToUserIdStr': 1,
            'idStr': 0
        }
        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")
        return field_map[field]
