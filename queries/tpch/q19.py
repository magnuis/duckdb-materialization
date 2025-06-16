from queries.query import Query


class Q19(Query):
    """
    TPC-H Query 19
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 19, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    SUM({self._json(tbl='l', col='l_extendedprice', fields=fields)} * (1 - {self._json(tbl='l', col='l_discount', fields=fields)})) AS revenue
FROM
    test_table l,
    test_table p
WHERE
    (
        {self._json(tbl='p', col='p_partkey', fields=fields)} = {self._json(tbl='l', col='l_partkey', fields=fields)}
        AND {self._json(tbl='p', col='p_brand', fields=fields)} = 'Brand#12'
        AND {self._json(tbl='p', col='p_container', fields=fields)} IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND {self._json(tbl='l', col='l_quantity', fields=fields)} BETWEEN 1 AND 11
        AND {self._json(tbl='p', col='p_size', fields=fields)} BETWEEN 1 AND 5
        AND {self._json(tbl='l', col='l_shipmode', fields=fields)} IN ('AIR', 'AIR REG')
        AND {self._json(tbl='l', col='l_shipinstruct', fields=fields)} = 'DELIVER IN PERSON'
    )
    OR
    (
        {self._json(tbl='p', col='p_partkey', fields=fields)} = {self._json(tbl='l', col='l_partkey', fields=fields)}
        AND {self._json(tbl='p', col='p_brand', fields=fields)} = 'Brand#23'
        AND {self._json(tbl='p', col='p_container', fields=fields)} IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND {self._json(tbl='l', col='l_quantity', fields=fields)} BETWEEN 10 AND 20
        AND {self._json(tbl='p', col='p_size', fields=fields)} BETWEEN 1 AND 10
        AND {self._json(tbl='l', col='l_shipmode', fields=fields)} IN ('AIR', 'AIR REG')
        AND {self._json(tbl='l', col='l_shipinstruct', fields=fields)} = 'DELIVER IN PERSON'
    )
    OR
    (
        {self._json(tbl='p', col='p_partkey', fields=fields)} = {self._json(tbl='l', col='l_partkey', fields=fields)}
        AND {self._json(tbl='p', col='p_brand', fields=fields)} = 'Brand#34'
        AND {self._json(tbl='p', col='p_container', fields=fields)} IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND {self._json(tbl='l', col='l_quantity', fields=fields)} BETWEEN 20 AND 30
        AND {self._json(tbl='p', col='p_size', fields=fields)} BETWEEN 1 AND 15
        AND {self._json(tbl='l', col='l_shipmode', fields=fields)} IN ('AIR', 'AIR REG')
        AND {self._json(tbl='l', col='l_shipinstruct', fields=fields)} = 'DELIVER IN PERSON'
    );

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
                "l_extendedprice",
                "l_discount"
            ],
            'where': [
                "p_brand",
                "p_container",
                "l_quantity",
                "p_size",
                "l_shipmode",
                "l_shipinstruct"
            ],
            'group_by': [],
            'order_by': [],
            'join': {
                "p_partkey": ["l_partkey"],
                "l_partkey": ["p_partkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "p_partkey": False,
            "l_partkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "p_brand": 0,
            "p_container": 0,
            "l_quantity": 0,
            "p_size": 0,
            "l_shipmode": 1,
            "l_shipinstruct": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "p_partkey": 1,
            "l_partkey": 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
