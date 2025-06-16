from queries.query import Query


class Q18(Query):
    """
    TPC-H Query 18
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 18, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='c', col='c_name', fields=fields)} AS c_name,
    {self._json(tbl='c', col='c_custkey', fields=fields)} AS c_custkey,
    {self._json(tbl='o', col='o_orderkey', fields=fields)} AS o_orderkey,
    {self._json(tbl='o', col='o_orderdate', fields=fields)} AS o_orderdate,
    {self._json(tbl='o', col='o_totalprice', fields=fields)} AS o_totalprice,
    SUM({self._json(tbl='l', col='l_quantity', fields=fields)}) AS total_quantity
FROM
    test_table c,
    test_table o,
    test_table l
WHERE
    {self._json(tbl='o', col='o_orderkey', fields=fields)} IN (
        SELECT
            {self._json(tbl='l', col='l_orderkey', fields=fields)}
        FROM
            test_table l
        GROUP BY
            {self._json(tbl='l', col='l_orderkey', fields=fields)}
        HAVING
            SUM({self._json(tbl='l', col='l_quantity', fields=fields)}) > 300
    )
    AND {self._json(tbl='c', col='c_custkey', fields=fields)} = {self._json(tbl='o', col='o_custkey', fields=fields)}
    AND {self._json(tbl='o', col='o_orderkey', fields=fields)} = {self._json(tbl='l', col='l_orderkey', fields=fields)}
GROUP BY
    {self._json(tbl='c', col='c_name', fields=fields)},
    {self._json(tbl='c', col='c_custkey', fields=fields)},
    {self._json(tbl='o', col='o_orderkey', fields=fields)},
    {self._json(tbl='o', col='o_orderdate', fields=fields)},
    {self._json(tbl='o', col='o_totalprice', fields=fields)}
ORDER BY
    {self._json(tbl='o', col='o_totalprice', fields=fields)} DESC,
    {self._json(tbl='o', col='o_orderdate', fields=fields)}
LIMIT
    100;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 2

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
                "c_name",
                "c_custkey",
                "o_orderkey",
                "o_orderdate",
                "o_totalprice",
                "l_quantity",
                "l_orderkey",
                "l_quantity"  # Fra HAVING
            ],
            'where': [
                "o_orderkey",
                "l_quantity"
            ],
            'group_by': [
                "l_orderkey"
                "c_name",
                "c_custkey",
                "o_orderkey",
                "o_orderdate",
                "o_totalprice"
            ],
            'order_by': [
                "o_totalprice",
                "o_orderdate"
            ],
            'join': {
                "c_custkey": ["o_custkey"],
                "o_custkey": ["c_custkey"],
                "l_orderkey": ["o_orderkey"],
                "o_orderkey": ["l_orderkey"]
            },

        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "c_custkey": False,
            "o_custkey": False,
            "l_orderkey": False,
            "o_orderkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "o_orderkey": 0,
            "l_quantity": 0
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
            "o_custkey": 1,
            "l_orderkey": 1,
            "o_orderkey": 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
