from queries.query import Query


class Q19(Query):
    """
    TPC-H Query 19
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "l": ["l_extendedprice", "l_discount", "l_quantity", "l_shipmode", "l_shipinstruct", "l_partkey"],
            "p": ["p_brand", "p_container", "p_size", "p_partkey"]
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 19, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    SUM({self._json(tbl='l', col='l_extendedprice', dts=dts)} * (1 - {self._json(tbl='l', col='l_discount', dts=dts)})) AS revenue
FROM
    extracted l,
    extracted p
WHERE
    (
        {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='l', col='l_partkey', dts=dts)}
        AND {self._json(tbl='p', col='p_brand', dts=dts)} = 'Brand#12'
        AND {self._json(tbl='p', col='p_container', dts=dts)} IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND {self._json(tbl='l', col='l_quantity', dts=dts)} BETWEEN 1 AND 11
        AND {self._json(tbl='p', col='p_size', dts=dts)} BETWEEN 1 AND 5
        AND {self._json(tbl='l', col='l_shipmode', dts=dts)} IN ('AIR', 'AIR REG')
        AND {self._json(tbl='l', col='l_shipinstruct', dts=dts)} = 'DELIVER IN PERSON'
    )
    OR
    (
        {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='l', col='l_partkey', dts=dts)}
        AND {self._json(tbl='p', col='p_brand', dts=dts)} = 'Brand#23'
        AND {self._json(tbl='p', col='p_container', dts=dts)} IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND {self._json(tbl='l', col='l_quantity', dts=dts)} BETWEEN 10 AND 20
        AND {self._json(tbl='p', col='p_size', dts=dts)} BETWEEN 1 AND 10
        AND {self._json(tbl='l', col='l_shipmode', dts=dts)} IN ('AIR', 'AIR REG')
        AND {self._json(tbl='l', col='l_shipinstruct', dts=dts)} = 'DELIVER IN PERSON'
    )
    OR
    (
        {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='l', col='l_partkey', dts=dts)}
        AND {self._json(tbl='p', col='p_brand', dts=dts)} = 'Brand#34'
        AND {self._json(tbl='p', col='p_container', dts=dts)} IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND {self._json(tbl='l', col='l_quantity', dts=dts)} BETWEEN 20 AND 30
        AND {self._json(tbl='p', col='p_size', dts=dts)} BETWEEN 1 AND 15
        AND {self._json(tbl='l', col='l_shipmode', dts=dts)} IN ('AIR', 'AIR REG')
        AND {self._json(tbl='l', col='l_shipinstruct', dts=dts)} = 'DELIVER IN PERSON'
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

    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "p_brand": False,
            "p_container": False,
            "l_quantity": False,
            "p_size": False,
            "l_shipmode": True,
            "l_shipinstruct": True
        }

        return field_map[field]
