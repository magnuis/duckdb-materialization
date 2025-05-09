from queries.query import Query


class Q19(Query):
    """
    TPC-H Query 19
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 19, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])})) AS revenue
FROM
    test_table l,
    test_table p
WHERE
    (
        {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
        AND {self._json(tbl='p', col='p_brand', dt=dts['p_brand'])} = 'Brand#12'
        AND {self._json(tbl='p', col='p_container', dt=dts['p_container'])} IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND {self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])} BETWEEN 1 AND 11
        AND {self._json(tbl='p', col='p_size', dt=dts['p_size'])} BETWEEN 1 AND 5
        AND {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])} IN ('AIR', 'AIR REG')
        AND {self._json(tbl='l', col='l_shipinstruct', dt=dts['l_shipinstruct'])} = 'DELIVER IN PERSON'
    )
    OR
    (
        {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
        AND {self._json(tbl='p', col='p_brand', dt=dts['p_brand'])} = 'Brand#23'
        AND {self._json(tbl='p', col='p_container', dt=dts['p_container'])} IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND {self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])} BETWEEN 10 AND 20
        AND {self._json(tbl='p', col='p_size', dt=dts['p_size'])} BETWEEN 1 AND 10
        AND {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])} IN ('AIR', 'AIR REG')
        AND {self._json(tbl='l', col='l_shipinstruct', dt=dts['l_shipinstruct'])} = 'DELIVER IN PERSON'
    )
    OR
    (
        {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
        AND {self._json(tbl='p', col='p_brand', dt=dts['p_brand'])} = 'Brand#34'
        AND {self._json(tbl='p', col='p_container', dt=dts['p_container'])} IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND {self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])} BETWEEN 20 AND 30
        AND {self._json(tbl='p', col='p_size', dt=dts['p_size'])} BETWEEN 1 AND 15
        AND {self._json(tbl='l', col='l_shipmode', dt=dts['l_shipmode'])} IN ('AIR', 'AIR REG')
        AND {self._json(tbl='l', col='l_shipinstruct', dt=dts['l_shipinstruct'])} = 'DELIVER IN PERSON'
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
