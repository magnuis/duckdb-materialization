from queries.query import Query


class Q1(Query):
    """
    Yelp Query 1
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Yelp query 1, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
    SELECT 
        COUNT({self._json(tbl='u', col='user_id', dt=dts['user_id'])}) AS cnt,
        {self._json(tbl='u', col='user_id', dt=dts['user_id'])} AS uid,
        {self._json(tbl='u', col='name', dt=dts['name'])} AS name,
        {self._json(tbl='u', col='average_stars', dt=dts['average_stars'])} AS avg_stars
    FROM 
        test_table u, 
        test_table r
    WHERE 
        {self._json(tbl='r', col='user_id', dt=dts['user_id'])} = {self._json(tbl='u', col='user_id', dt=dts['user_id'])}
        AND {self._json(tbl='u', col='name', dt=dts['name'])} IS NOT NULL
        AND {self._json(tbl='u', col='average_stars', dt=dts['average_stars'])} IS NOT NULL
        AND {self._json(tbl='r', col='review_id', dt=dts['review_id'])} IS NOT NULL
    GROUP BY 
        {self._json(tbl='u', col='user_id', dt=dts['user_id'])},
        {self._json(tbl='u', col='name', dt=dts['name'])},
        {self._json(tbl='u', col='average_stars', dt=dts['average_stars'])}
    ORDER BY 
        cnt DESC, 
        {self._json(tbl='u', col='name', dt=dts['name'])}
    LIMIT 10;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 1

    # TODO
    def columns_used_with_position(self,) -> dict[str, list[str]]:
        """
        Get the columns used in Yelp Query 1 along with their position in the query 
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
                "name",
                "average_stars"
            ],
            'where': [
                "name",
                "average_stars",
                "review_id"
            ],
            'group_by': [
                "user_id",
                "name",
                "average_stars"
            ],
            'order_by': [
                "name"
            ],
            'join': {
                "user_id": ["user_id"]

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
