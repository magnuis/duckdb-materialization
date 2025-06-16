from queries.query import Query


class Q6(Query):
    """
    Twitter Query 6
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 6, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
            SELECT 
                {self._json(col='idStr', tbl='initial_tweet', fields=fields)} AS initial_tweet_id,
                {self._json(col='retweetedStatus_user_screenName', tbl='initial_tweet', fields=fields)} AS initial_author,
                {self._json(col='retweetedStatus_user_screenName', tbl='retweet1', fields=fields)} AS first_retweeter,
                {self._json(col='retweetedStatus_user_screenName', tbl='retweet2', fields=fields)} AS second_retweeter,
            FROM test_table AS initial_tweet
            JOIN test_table AS retweet1 
                ON {self._json(col='retweetedStatus_idStr', tbl='retweet1', fields=fields)} = {self._json(col='idStr', tbl='initial_tweet', fields=fields)}
            JOIN test_table AS retweet2 
                ON {self._json(col='retweetedStatus_idStr', tbl='retweet2', fields=fields)} = {self._json(col='idStr', tbl='retweet1', fields=fields)}
            WHERE 
                NOT {self._json(col='user_isTranslator', tbl='initial_tweet', fields=fields)}
            ORDER BY initial_tweet_id
            LIMIT 20;
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
                'idStr',
                'retweetedStatus_user_screenName',
                'retweetedStatus_user_screenName',
                'retweetedStatus_user_screenName'
            ],
            'where': [
                "user_isTranslator"
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {
                "retweetedStatus_idStr": ['idStr', 'idStr'],
                "idStr": ['retweetedStatus_idStr, retweetedStatus_idStr']
            }
        }

    def get_field_weight(self, field: str, prev_materialization: list[str]) -> int:
        field_map = {
            'retweetedStatus_idStr': 2*self.good_field_weight,
            "user_isTranslator": 1 * self.good_field_weight,
            'idStr':  3*self.poor_field_weight,
            "retweetedStatus_user_screenName": 3*self.poor_field_weight
        }
        if field not in field_map:
            raise ValueError(f"{field} not a query field")

        return field_map.get(field, 0)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            'user_isTranslator': 1
        }

        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            'retweetedStatus_idStr': 2,
            'idStr': 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
