from queries.query import Query


class Q7(Query):
    """
    Twitter Query 7
    """

    def __init__(self):
        super().__init__()

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 7, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_types(fields=fields)
        acs = self._get_field_accesses(fields=fields)

        return f"""
        SELECT 
            {self._json(col='idStr', tbl='original_tweet', dt=dts['idStr'], acs=acs['idStr'])} AS original_tweet_id,
            {self._json(col='user_screenName', tbl='original_tweet', dt=dts['user_screenName'], acs=acs['user_screenName'])} AS original_author,
            {self._json(col='user_screenName', tbl='retweet', dt=dts['user_screenName'], acs=acs['user_screenName'])} AS retweeter,
            {self._json(col='retweetedStatus_retweetCount', tbl='retweet', dt=dts['retweetedStatus_retweetCount'], acs=acs['retweetedStatus_retweetCount'])} AS retweet_retweet_count
        FROM 
            test_table AS original_tweet, 
            test_table AS retweet
        WHERE 
            {self._json(col='retweetedStatus_idStr', tbl='retweet', dt=dts['retweetedStatus_idStr'], acs=acs['retweetedStatus_idStr'])} = {self._json(col='idStr', tbl='original_tweet', dt=dts['idStr'], acs=acs['idStr'])}
        GROUP BY 
            original_tweet_id,
            original_author,
            retweeter,
            retweet_retweet_count
        ORDER BY 
            retweet_retweet_count 
            DESC
        LIMIT 10;
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 0

    # TODO
    def columns_used_with_position(self,) -> dict[str, list[str]]:
        """
        Get the columns used in Twitter Query 7 along with their position in the query 
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
                'user_screenName',
                'user_screenName',
                'retweetedStatus_retweetCount'
            ],
            'where': [

            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {
                'retweetedStatus_idStr': ['idStr'],
                'idStr': ['retweetedStatus_idStr']

            }
        }

    def get_field_weight(self, field: str, prev_materialization: list[str]) -> int:
        field_map = {
            'retweetedStatus_idStr': 1*self.GOOD_FIELD_WEIGHT,
            "idStr": 1 * self.GOOD_FIELD_WEIGHT,
            'user_screenName':  2*self.POOR_FIELD_WEIGHT,
            "retweetedStatus_retweetCount": 1*self.POOR_FIELD_WEIGHT
        }
        if field not in field_map:
            raise ValueError(f"{field} not a query field")

        return field_map.get(field, 0)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
        }

        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            'retweetedStatus_idStr': 1,
            'idStr': 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
