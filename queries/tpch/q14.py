from queries.query import Query


class Q14(Query):
    """
    TPC-H Query 14
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "p": ["p_partkey", "p_type"],
            "l": ["l_extendedprice", "l_discount", "l_partkey", "l_shipdate"],
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 14, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    100.00 * SUM(
        CASE
            WHEN {self._json(tbl='p', col='p_type', dts=dts)} LIKE 'PROMO%' THEN {self._json(tbl='l', col='l_extendedprice', dts=dts)} * (1 - {self._json(tbl='l', col='l_discount', dts=dts)})
            ELSE 0
        END
    ) / SUM({self._json(tbl='l', col='l_extendedprice', dts=dts)} * (1 - {self._json(tbl='l', col='l_discount', dts=dts)})) AS promo_revenue
FROM
    extracted l,
    extracted p
WHERE
    {self._json(tbl='l', col='l_partkey', dts=dts)} = {self._json(tbl='p', col='p_partkey', dts=dts)} 
    AND {self._json(tbl='l', col='l_shipdate', dts=dts)} >= DATE '1995-09-01'
    AND {self._json(tbl='l', col='l_shipdate', dts=dts)} < DATE '1995-10-01';

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
                "p_type",
                "l_extendedprice",
                "l_discount"
            ],
            'where': [
                "l_shipdate"
            ],
            'group_by': [],
            'order_by': [],
            'join': {
                "l_partkey": ["p_partkey"],
                "p_partkey": ["l_partkey"],
            }
        }
