from queries.query import Query


class Q21(Query):
    """
    TPC-H Query 21
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "s": ["s_nationkey", "s_name", "s_suppkey"],
            "o": ["o_orderstatus", "o_orderkey"],
            "n": ["n_nationkey", "n_name"],
            "l1": ["l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate"],
            "l2": ["l_orderkey", "l_suppkey"],
            "l3": ["l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate"],
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 21, adjusted to current db materializaiton

        Returns
        -------
        str
        """


#         return f"""
# SELECT
#     {self._json(tbl='s', col='s_name', dts=dts)} AS s_name,
#     COUNT(*) AS numwait
# FROM
#     s,
#     l1,
#     o,
#     n
# WHERE
#     {self._json(tbl='s', col='s_suppkey', dts=dts)} = {self._json(tbl='l1', col='l_suppkey', dts=dts)}
#     AND {self._json(tbl='o', col='o_orderkey', dts=dts)} = {self._json(tbl='l1', col='l_orderkey', dts=dts)}
#     AND {self._json(tbl='o', col='o_orderstatus', dts=dts)} = 'F'
#     AND {self._json(tbl='l1', col='l_receiptdate', dts=dts)} > {self._json(tbl='l1', col='l_commitdate', dts=dts)}
#     AND EXISTS (
#         SELECT
#             *
#         FROM
#             l2
#         WHERE
#             {self._json(tbl='l2', col='l_orderkey', dts=dts)} = {self._json(tbl='l1', col='l_orderkey', dts=dts)}
#             AND {self._json(tbl='l2', col='l_suppkey', dts=dts)} <> {self._json(tbl='l1', col='l_suppkey', dts=dts)}
#     )
#     AND NOT EXISTS (
#         SELECT
#             *
#         FROM
#             l3
#         WHERE
#             {self._json(tbl='l3', col='l_orderkey', dts=dts)} = {self._json(tbl='l1', col='l_orderkey', dts=dts)}
#             AND {self._json(tbl='l3', col='l_suppkey', dts=dts)} <> {self._json(tbl='l1', col='l_suppkey', dts=dts)}
#             AND {self._json(tbl='l3', col='l_receiptdate', dts=dts)} > {self._json(tbl='l3', col='l_commitdate', dts=dts)}
#     )
#     AND {self._json(tbl='s', col='s_nationkey', dts=dts)} = {self._json(tbl='n', col='n_nationkey', dts=dts)}
#     AND {self._json(tbl='n', col='n_name', dts=dts)} = 'SAUDI ARABIA'
# GROUP BY
#     {self._json(tbl='s', col='s_name', dts=dts)}
# ORDER BY
#     numwait DESC,
#     {self._json(tbl='s', col='s_name', dts=dts)}
# LIMIT
#     100;
#     """

# REWRITING TO AVOID CROSS PRODUCTS
        return f"""
  , late_l1 AS (
    SELECT *
    FROM extracted AS l1
    WHERE {self._json(tbl='l1', col='l_receiptdate', dts=dts)} 
      > {self._json(tbl='l1', col='l_commitdate', dts=dts)}
  ),

  orders_with_other AS (
    SELECT DISTINCT
      {self._json(tbl='l2', col='l_orderkey', dts=dts)} AS l_orderkey
    FROM extracted AS l2
    JOIN late_l1 AS l1
      ON {self._json(tbl='l2', col='l_orderkey', dts=dts)} 
         = {self._json(tbl='l1', col='l_orderkey', dts=dts)}
     AND {self._json(tbl='l2', col='l_suppkey', dts=dts)}  
         <> {self._json(tbl='l1', col='l_suppkey', dts=dts)}
  ),

  orders_with_late_other AS (
    SELECT DISTINCT
      {self._json(tbl='l3', col='l_orderkey', dts=dts)} AS l_orderkey
    FROM extracted AS l3
    JOIN late_l1 AS l1
      ON {self._json(tbl='l3', col='l_orderkey', dts=dts)} 
         = {self._json(tbl='l1', col='l_orderkey', dts=dts)}
     AND {self._json(tbl='l3', col='l_suppkey', dts=dts)}  
         <> {self._json(tbl='l1', col='l_suppkey', dts=dts)}
    WHERE {self._json(tbl='l3', col='l_receiptdate', dts=dts)} 
          > {self._json(tbl='l3', col='l_commitdate', dts=dts)}
  )

SELECT
    {self._json(tbl='s', col='s_name', dts=dts)} AS s_name,
    COUNT(*)                               AS numwait
FROM late_l1 AS l1

  -- enforce “exists another supp” by an inner‐join to that CTE
  JOIN orders_with_other AS owh
    ON {self._json(tbl='l1', col='l_orderkey', dts=dts)} 
       = owh.l_orderkey

  LEFT JOIN orders_with_late_other AS owlo
    ON {self._json(tbl='l1', col='l_orderkey', dts=dts)} 
       = owlo.l_orderkey

  JOIN extracted AS s
    ON {self._json(tbl='s', col='s_suppkey', dts=dts)} 
       = {self._json(tbl='l1', col='l_suppkey', dts=dts)}

  JOIN extracted AS o
    ON {self._json(tbl='o', col='o_orderkey', dts=dts)} 
       = {self._json(tbl='l1', col='l_orderkey', dts=dts)}
   AND {self._json(tbl='o', col='o_orderstatus', dts=dts)} = 'F'

  JOIN extracted AS n
    ON {self._json(tbl='n', col='n_nationkey', dts=dts)} 
       = {self._json(tbl='s', col='s_nationkey', dts=dts)}
   AND {self._json(tbl='n', col='n_name', dts=dts)}      = 'SAUDI ARABIA'

WHERE
  owlo.l_orderkey IS NULL

GROUP BY
    {self._json(tbl='s', col='s_name', dts=dts)}

ORDER BY
    numwait DESC,
    {self._json(tbl='s', col='s_name', dts=dts)}

LIMIT 100;
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
                "s_name"
            ],
            'where': [
                "o_orderstatus",
                "l_receiptdate",
                "l_commitdate",
                "l_receiptdate",
                "l_commitdate",
                "n_name"
            ],
            'group_by': [
                "s_name"
            ],
            'order_by': [
                "s_name"
            ],
            'join': {
                "s_suppkey": ["l_suppkey"],
                "l_suppkey": ["s_suppkey"],
                "o_orderkey": ["l_orderkey"],
                "l_orderkey": ["o_orderkey"],
                "s_nationkey": ["n_nationkey"],
                "n_nationkey": ["s_nationkey"]
            },
            "self_join": {
                "l_orderkey": 2,
                "l_suppkey": 2
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "s_suppkey": False,
            "l_suppkey": False,
            "o_orderkey": True,
            "l_orderkey": False,
            "s_nationkey": False,
            "n_nationkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "o_orderstatus": True,
            "l_receiptdate": False,
            "l_commitdate": False,
            "n_name": True
        }

        return field_map[field]
