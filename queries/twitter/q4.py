from queries.query import Query


class Q4(Query):
    """
    Twitter Query 4
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted Twitter query 4, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_types(fields=fields)
        acs = self._get_field_accesses(fields=fields)

        return f"""
            SELECT 
                {self._json(col='inReplyToUserIdStr', tbl='t', dt=dts['inReplyToUserIdStr'], acs=acs['inReplyToUserIdStr'])} AS user_id,
                COUNT(*) AS reply_count
            FROM test_table t
            WHERE 
                {self._json(col='inReplyToUserIdStr', tbl='t', dt=dts['inReplyToUserIdStr'], acs=acs['inReplyToUserIdStr'])} IS NOT NULL
            GROUP BY {self._json(col='inReplyToUserIdStr', tbl='t', dt=dts['inReplyToUserIdStr'], acs=acs['inReplyToUserIdStr'])}
            ORDER BY reply_count DESC
            LIMIT 15;
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 0

    # TODO
    def columns_used_with_position(self,) -> dict[str, list[str]]:
        """
        Get the columns used in Twitter Query 4 along with their position in the query 
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
                'inReplyToUserIdStr'
            ],
            'where': [
                "inReplyToUserIdStr"
            ],
            'group_by': [
                'inReplyToUserIdStr'
            ],
            'order_by': [
            ],
            'join': {

            }
        }

    # TODO
    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            'inReplyToUserIdStr': 1
        }

        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {}

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
