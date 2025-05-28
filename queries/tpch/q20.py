from queries.query import Query


class Q20(Query):
    """
    TPC-H Query 20
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "l": ["l_quantity", "l_shipdate", "l_suppkey", "l_partkey"],
            "p": ["p_partkey", "p_name"],
            "ps": ["ps_partkey", "ps_availqty", "ps_suppkey"],
            "n": ["n_name", "n_nationkey"],
            "s": ["s_name", "s_address", "s_suppkey", "s_nationkey"]
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 20, adjusted to current db materializaiton

        Returns
        -------
        str
        """

#         return f"""
# SELECT
#     {self._json(tbl='s', col='s_name', dts=dts)} AS s_name,
#     {self._json(tbl='s', col='s_address', dts=dts)} AS s_address
# FROM
#     extracted s,
#     extracted n
# WHERE
#     {self._json(tbl='s', col='s_suppkey', dts=dts)} IN (
#         SELECT
#             {self._json(tbl='ps', col='ps_suppkey', dts=dts)}
#         FROM
#             extracted ps
#         WHERE
#             {self._json(tbl='ps', col='ps_partkey', dts=dts)} IN (
#                 SELECT
#                     {self._json(tbl='p', col='p_partkey', dts=dts)}
#                 FROM
#                     extracted p
#                 WHERE
#                     {self._json(tbl='p', col='p_name', dts=dts)} LIKE 'forest%'
#             )
#             AND {self._json(tbl='ps', col='ps_availqty', dts=dts)} > (
#                 SELECT
#                     0.5 * SUM({self._json(tbl='l', col='l_quantity', dts=dts)})
#                 FROM
#                     extracted l
#                 WHERE
#                     {self._json(tbl='l', col='l_partkey', dts=dts)} = {self._json(tbl='ps', col='ps_partkey', dts=dts)}
#                     AND {self._json(tbl='l', col='l_suppkey', dts=dts)} = {self._json(tbl='ps', col='ps_suppkey', dts=dts)}
#                     AND {self._json(tbl='l', col='l_shipdate', dts=dts)} >= DATE '1994-01-01'
#                     AND {self._json(tbl='l', col='l_shipdate', dts=dts)} < DATE '1995-01-01'
#             )
#     )
#     AND {self._json(tbl='s', col='s_nationkey', dts=dts)} = {self._json(tbl='n', col='n_nationkey', dts=dts)}
#     AND {self._json(tbl='n', col='n_name', dts=dts)} = 'CANADA'
# ORDER BY
#     {self._json(tbl='s', col='s_name', dts=dts)};
#     """
        return f"""
  , p_forest AS (
    SELECT
      {self._json(tbl='p', col='p_partkey', dts=dts)} AS partkey
    FROM extracted AS p
    WHERE {self._json(tbl='p', col='p_name', dts=dts)} LIKE 'forest%'
  ),

  supply_sum AS (
    SELECT
      {self._json(tbl='l', col='l_partkey', dts=dts)} AS partkey,
      {self._json(tbl='l', col='l_suppkey', dts=dts)} AS suppkey,
      SUM({self._json(tbl='l', col='l_quantity', dts=dts)})        AS total_qty
    FROM extracted AS l
    WHERE
      {self._json(tbl='l', col='l_shipdate', dts=dts)} >= DATE '1994-01-01'
      AND {self._json(tbl='l', col='l_shipdate', dts=dts)} <  DATE '1995-01-01'
    GROUP BY
      partkey, suppkey
  ),

  ps_available AS (
    SELECT
      {self._json(tbl='ps', col='ps_partkey', dts=dts)} AS partkey,
      {self._json(tbl='ps', col='ps_suppkey', dts=dts)} AS suppkey
    FROM extracted AS ps
    JOIN p_forest AS pf
      ON {self._json(tbl='ps', col='ps_partkey', dts=dts)} = pf.partkey
    JOIN supply_sum AS ss
      ON ss.partkey = {self._json(tbl='ps', col='ps_partkey', dts=dts)}
     AND ss.suppkey = {self._json(tbl='ps', col='ps_suppkey', dts=dts)}
    WHERE
      {self._json(tbl='ps', col='ps_availqty', dts=dts)} > 0.5 * ss.total_qty
  )

SELECT
    {self._json(tbl='s', col='s_name',   dts=dts)} AS s_name,
    {self._json(tbl='s', col='s_address', dts=dts)} AS s_address
FROM extracted AS s

  JOIN ps_available AS pa
    ON {self._json(tbl='s', col='s_suppkey', dts=dts)} = pa.suppkey

  JOIN extracted AS n
    ON {self._json(tbl='s', col='s_nationkey', dts=dts)} = {self._json(tbl='n', col='n_nationkey', dts=dts)}
   AND {self._json(tbl='n', col='n_name',      dts=dts)} = 'CANADA'

ORDER BY
    {self._json(tbl='s', col='s_name', dts=dts)};
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
                "ps_partkey",
                "p_name",
                "ps_availqty",
                "l_shipdate",
                "n_name",
            ],
            'group_by': [],
            'order_by': [
                "s_name"
            ],
            'join': {
                "l_partkey": ["ps_partkey"],
                "ps_partkey": ["l_partkey"],
                "l_suppkey": ["ps_suppkey"],
                "ps_suppkey": ["l_suppkey"],
                "s_nationkey": ["n_nationkey"],
                "n_nationkey": ["s_nationkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "l_partkey": True,
            "ps_partkey": False,
            "l_suppkey": True,
            "ps_suppkey": False,
            "s_nationkey": False,
            "n_nationkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "s_suppkey": False,
            "ps_partkey": False,
            "p_name": True,
            "ps_availqty": False,
            "l_shipdate": True,
            "n_name": True
        }

        return field_map[field]
