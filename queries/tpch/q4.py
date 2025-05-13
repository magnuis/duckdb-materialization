from queries.query import Query


class Q4(Query):
    """
    TPC-H Query 4
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "o": ["o_orderpriority", "o_orderdate", "o_orderkey"],
            "l": ["l_orderkey", "l_commitdate", "l_receiptdate"]
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 4, adjusted to current db materializaiton

        Returns
        -------
        str
        """

#         return f"""
# SELECT
#     {self._json(tbl='o', col='o_orderpriority', dts=dts)} AS o_orderpriority,
#     COUNT(*) AS order_count
# FROM
#     extracted o
# WHERE
#     {self._json(tbl='o', col='o_orderdate', dts=dts)} >= DATE '1993-07-01'
#     AND {self._json(tbl='o', col='o_orderdate', dts=dts)} < DATE '1993-07-01' + INTERVAL '3' MONTH
#     AND EXISTS (
#         SELECT
#             *
#         FROM
#             extracted l
#         WHERE
#             {self._json(tbl='l', col='l_orderkey', dts=dts)} = {self._json(tbl='o', col='o_orderkey', dts=dts)}
#             AND {self._json(tbl='l', col='l_commitdate', dts=dts)} < {self._json(tbl='l', col='l_receiptdate', dts=dts)}
#     )
# GROUP BY
#     o_orderpriority
# ORDER BY
#     o_orderpriority;
#     """

# REWRITE TO AVOID CROSS PRODUCT
        return f"""
 , late_orders AS (
  SELECT DISTINCT
    {self._json(tbl='l', col='l_orderkey', dts=dts)} AS l_orderkey
  FROM extracted AS l
  WHERE
    {self._json(tbl='l', col='l_commitdate', dts=dts)} 
      < {self._json(tbl='l', col='l_receiptdate', dts=dts)}
)

SELECT
    {self._json(tbl='o', col='o_orderpriority', dts=dts)} AS o_orderpriority,
    COUNT(*)                                           AS order_count
FROM extracted AS o

  JOIN late_orders AS lo
    ON {self._json(tbl='o', col='o_orderkey', dts=dts)} = lo.l_orderkey

WHERE
    {self._json(tbl='o', col='o_orderdate', dts=dts)} >= DATE '1993-07-01'
    AND {self._json(tbl='o', col='o_orderdate', dts=dts)} <  DATE '1993-07-01' + INTERVAL '3' MONTH

GROUP BY
    {self._json(tbl='o', col='o_orderpriority', dts=dts)}

ORDER BY
    {self._json(tbl='o', col='o_orderpriority', dts=dts)};
"""

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 1

    def columns_used_with_position(self) -> dict[str, list[str]]:
        """
        Get the columns used in the query along with their position in the query 
        (e.g., SELECT, WHERE, GROUP BY, ORDER BY clauses).

        Returns
        -------
        dict
            A dictionary with the following keys:
            - 'select': list of column names used in the SELECT clause.
            - 'where': list of column names used in the WHERE clause that are not joins.
            - 'group_by': list of column names used in the GROUP BY clause.
            - 'order_by': list of column names used in the ORDER BY clause.
            - 'join': list of column names used in a join operation (including WHERE)
        """
        return {
            'select': [
                "o_orderpriority"
            ],
            'where': [
                "o_orderdate",
                "l_commitdate",
                "l_receiptdate"
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {
                "l_orderkey": ["o_orderkey"],
                "o_orderkey": ["l_orderkey"]
            }
        }
