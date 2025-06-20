from queries.query import Query


class Q13(Query):
    """
    TPC-H Query 13
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 13, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            {self._json(tbl='c', col='c_custkey', fields=fields)} AS c_custkey,
            COUNT({self._json(tbl='o', col='o_orderkey', fields=fields)}) AS c_count
        FROM
            test_table c LEFT OUTER JOIN test_table o ON
                {self._json(tbl='c', col='c_custkey', fields=fields)} = {self._json(tbl='o', col='o_custkey', fields=fields)}
                AND {self._json(tbl='o', col='o_comment', fields=fields)} NOT LIKE '%special%requests%'
        GROUP BY
            {self._json(tbl='c', col='c_custkey', fields=fields)}
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 1

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """
        Get the underlying column names used in the query along with their position
        in the query (e.g., SELECT, WHERE, GROUP BY, ORDER BY clauses).

        Returns
        -------
        dict
            A dictionary with the following keys:
            - 'select': list of underlying column names used in the SELECT clause.
            - 'where': list of underlying column names used in the WHERE clause that are not joins.
            - 'group_by': list of underlying column names used in the GROUP BY clause.
            - 'order_by': list of underlying column names used in the ORDER BY clause.
            - 'join': list of underlying column names used in a join operation (including WHERE)
        """
        return {
            'select': [
                "c_custkey",
                "o_orderkey"
            ],
            'where': [
                "o_comment"
            ],
            'group_by': [
                "c_custkey"
            ],
            'order_by': [
            ],
            'join': {
                "c_custkey": ["o_custkey"],
                "o_custkey": ["c_custkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "c_custkey": False,
            "o_custkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "o_comment": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "c_custkey": 1,
            "o_custkey": 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
