from queries.query import Query


class Q20(Query):
    """
    TPC-H Query 20
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 20, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    {self._json(tbl='s', col='s_name', dt=dts['s_name'])} AS s_name,
    {self._json(tbl='s', col='s_address', dt=dts['s_address'])} AS s_address
FROM
    test_table s,
    test_table n
WHERE
    {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} IN (
        SELECT
            {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])}
        FROM
            test_table ps
        WHERE
            {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])} IN (
                SELECT
                    {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])}
                FROM
                    test_table p
                WHERE
                    {self._json(tbl='p', col='p_name', dt=dts['p_name'])} LIKE 'forest%'
            )
            AND {self._json(tbl='ps', col='ps_availqty', dt=dts['ps_availqty'])} > (
                SELECT
                    0.5 * SUM({self._json(tbl='l', col='l_quantity', dt=dts['l_quantity'])})
                FROM
                    test_table l
                WHERE
                    {self._json(tbl='l', col='l_partkey', dt=dts['l_partkey'])} = {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])}
                    AND {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])} = {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])}
                    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} >= DATE '1994-01-01'
                    AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} < DATE '1995-01-01'
            )
    )
    AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
    AND {self._json(tbl='n', col='n_name', dt=dts['n_name'])} = 'CANADA'
ORDER BY
    {self._json(tbl='s', col='s_name', dt=dts['s_name'])};
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 3

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
                "s_name",
                "s_address",
                "ps_suppkey",
                "p_partkey",
                "l_quantity"
            ],
            'where': [
                "s_suppkey",
                "ps_partkey"
                "p_name",
                "ps_availqty",
                "l_shipdate",
                "n_name",
            ],
            'group_by': [],
            'order_by': [
                "s_name"
            ],
            'join': [
                "l_partkey",
                "ps_partkey",
                "l_suppkey",
                "ps_suppkey",
                "s_nationkey",
                "n_nationkey"
            ]
        }
