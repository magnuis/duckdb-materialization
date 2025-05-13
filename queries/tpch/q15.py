from queries.query import Query


class Q15(Query):
    """
    TPC-H Query 15s
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "l": ["l_suppkey", "l_extendedprice", "l_discount", "l_shipdate"],
            "s": ["s_suppkey", "s_name", "s_address", "s_phone"]
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 15s
, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
, revenue (supplier_no, total_revenue) AS (
    SELECT
        {self._json(tbl='l', col='l_suppkey', dts=dts)} AS l_suppkey,
        SUM({self._json(tbl='l', col='l_extendedprice', dts=dts)} * (1 - {self._json(tbl='l', col='l_discount', dts=dts)})) AS total_revenue
    FROM
        extracted l
    WHERE
        {self._json(tbl='l', col='l_shipdate', dts=dts)} >= DATE '1996-01-01'
        AND {self._json(tbl='l', col='l_shipdate', dts=dts)} < DATE '1996-04-01'
    GROUP BY
        {self._json(tbl='l', col='l_suppkey', dts=dts)}
)
SELECT
    {self._json(tbl='s', col='s_suppkey', dts=dts)} AS s_suppkey,
    {self._json(tbl='s', col='s_name', dts=dts)} AS s_name,
    {self._json(tbl='s', col='s_address', dts=dts)} AS s_address,
    {self._json(tbl='s', col='s_phone', dts=dts)} AS s_phone,
    total_revenue
FROM
    extracted s,
    revenue
WHERE
    {self._json(tbl='s', col='s_suppkey', dts=dts)} = revenue.supplier_no
    AND total_revenue = (
        SELECT
            MAX(total_revenue)
        FROM
            revenue
    )
ORDER BY
    {self._json(tbl='s', col='s_suppkey', dts=dts)};

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
                "l_suppkey",
                "l_extendedprice",
                "l_discount",
                "s_suppkey",
                "s_name",
                "s_address",
                "s_phone",
            ],
            'where': [
                "l_shipdate"
            ],
            'group_by': [
                "l_suppkey"
            ],
            'order_by': [
                "s_suppkey"
            ],
            'join': {
                "s_suppkey": [None],
            }
        }
