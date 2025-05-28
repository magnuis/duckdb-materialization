from queries.query import Query


class Q2(Query):
    """
    Yelp Query 2
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Yelp query 2, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
    SELECT 
        COUNT(DISTINCT {self._json(tbl='r', col='user_id', dt=dts['user_id'])}) AS cnt,
        {self._json(tbl='r', col='business_id', dt=dts['business_id'])} AS bid
    FROM test_table r
    WHERE 
        {self._json(tbl='r', col='date', dt=dts['date'])} > DATE '2017-01-01'
        AND {self._json(tbl='r', col='date', dt=dts['date'])} < DATE '2018-01-01'
        AND {self._json(tbl='r', col='review_id', dt=dts['review_id'])} IS NOT NULL
    GROUP BY {self._json(tbl='r', col='business_id', dt=dts['business_id'])}
    ORDER BY cnt DESC, bid
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
        Get the columns used in Yelp Query 2 along with their position in the query 
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
                "user_id",
                "business_id"
            ],
            'where': [
                "date",
                "review_id"
            ],
            'group_by': [
                "business_id"
            ],
            'order_by': [
            ],
            'join': {
            }
        }

    # TODO
    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "l_shipdate": True
        }

        return field_map[field]
