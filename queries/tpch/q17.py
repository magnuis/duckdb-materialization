from queries.query import Query


class Q17(Query):
    """
    TPC-H Query 17
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 17, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    SUM({self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])}) / 7.0 AS avg_yearly
FROM
    test_table l,
    test_table p
WHERE
    {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
    AND {self._json(tbl='p', col='p_brand', dt=dts['p_brand'])} = 'Brand#23'
    AND {self._json(tbl='p', col='p_container', dt=dts['p_container'])} = 'MED BOX'
    AND {self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])} < (
        SELECT
            0.2 * AVG({self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])})
        FROM
            test_table l
        WHERE
            {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])}
    );
    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 17

        Returns
        -------
        list[str]
        """

        return [
            "l_extendedprice",
            "l_partkey",
            "l_quantity",
            "p_partkey",
            "p_brand",
            "p_container"
        ]

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
                "l_quantity"
            ],
            'where': [
                "p_brand",
                "p_container",
                "l_quantity"
            ],
            'group_by': [
            ],
            'order_by': [],
            'join': {
                "p_partkey": ["l_partkey", "l_partkey"],
                "l_partkey": ["p_partkey", "p_partkey"]
            }
        }
