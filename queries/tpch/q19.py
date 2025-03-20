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

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 19

        Returns
        -------
        list[str]
        """

        return [
            "l_extendedprice",
            "l_discount",
            "l_partkey",
            "l_quantity",
            "l_shipmode",
            "l_shipinstruct",
            "p_partkey",
            "p_brand",
            "p_container",
            "p_size"
        ]
