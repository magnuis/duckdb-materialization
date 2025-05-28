from queries.query import Query


class Q3(Query):
    """
    Yelp Query 3
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Yelp query 3, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
    WITH citycount (city, cnt, userid) AS (
        SELECT 
            DISTINCT {self._json(tbl='b', col='city', dt=dts['city'])} AS city, COUNT(*) AS cnt,
            {self._json(tbl='r', col='user_id', dt=dts['user_id'])} AS userid
        FROM test_table b, test_table r
        WHERE 
            {self._json(tbl='r', col='business_id', dt=dts['business_id'])} = {self._json(tbl='b', col='business_id', dt=dts['business_id'])}
            AND {self._json(tbl='b', col='city', dt=dts['city'])} IS NOT NULL
            AND {self._json(tbl='r', col='review_id', dt=dts['review_id'])} IS NOT NULL
        GROUP BY 
            {self._json(tbl='b', col='city', dt=dts['city'])}, 
            {self._json(tbl='r', col='user_id', dt=dts['user_id'])}
    )
    SELECT
        c.city AS city,
        c.user_id AS userid,
        {self._json(tbl='u', col='name', dt=dts['name'])} AS username,
        c.cnt AS review_cnt
    FROM 
        citycount c, 
        test_table u, 
        (
            SELECT 
                c2.user_id AS userid,
                c2.city AS city
            FROM citycount c2 
            WHERE c2.cnt = (
                SELECT MAX(c3.cnt) 
                FROM citycount c3 
                WHERE c2.city = c3.city
            )
        ) cj
    WHERE 
        {self._json(tbl='u', col='name', dt=dts['name'])} IS NOT NULL
        AND {self._json(tbl='u', col='average_stars', dt=dts['average_stars'])} IS NOT NULL
        AND c.user_id = cj.user_id
        AND c.city = cj.city_id
        AND c.user_id = {self._json(tbl='u', col='user_id', dt=dts['user_id'])}
    ORDER BY c.cnt DESC, city, username
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
        Get the columns used in Yelp Query 3 along with their position in the query 
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
                "city",
                "user_id",
                "city",
                "user_id",
                "name",
                "city",
                "user_id",
            ],
            'where': [
                "vity",
                "review_id",
                "name",
                "average_stars",

            ],
            'group_by': [
                "city",
                "user_id",
            ],
            'order_by': [
            ],
            'join': {
                "business_id": ["business_id"],
                "city": ["city", "city", "city", "city"],
                "user_id": ["user_id", "user_id", "user_id"],
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


"""
with citycount (city, cnt, userid) as (
    select
       distinct b.raw_json->>'city' as city, 
       count(*) as cnt, r.raw_json->>'user_id' as userid
    from
        test_table b,
        test_table r
    where
        r.raw_json->>'business_id' = b.raw_json->>'business_id'
        and b.raw_json->>'city' is not null
        and r.raw_json->>'review_id' is not null
    group by
          b.raw_json->>'city', r.raw_json->>'user_id'
)

select
    c.city as city, c.userid as userid, u.raw_json->>'name' as username, c.cnt as review_cnt
from
    citycount c,
    test_table u,
    (select c2.userid as userid, c2.city as city from citycount c2 where c2.cnt = (select max(c3.cnt) from citycount c3 where c2.city = c3.city)) cj
where
    u.raw_json->>'name' is not null
    and cast(u.raw_json->>'average_stars' as float) is not null
    and c.userid = cj.userid
    and c.city = cj.city
    and c.userid = u.raw_json->>'user_id'
order by
    c.cnt
desc
limit 10;
"""
