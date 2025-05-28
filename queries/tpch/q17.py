from queries.query import Query


class Q17(Query):
    """
    TPC-H Query 17
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "l1": ["l_extendedprice", "l_quantity", "l_partkey"],
            "l2": ["l_quantity", "l_partkey"],
            "p": ["p_partkey", "p_brand", "p_container"],
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 17, adjusted to current db materializaiton

        Returns
        -------
        str
        """

#         return f"""
# SELECT
#     SUM({self._json(tbl='l1', col='l_extendedprice', dts=dts)}) / 7.0 AS avg_yearly
# FROM
#     extracted  l1,
#     extracted  p
# WHERE
#     {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='l1', col='l_partkey', dts=dts)}
#     AND {self._json(tbl='p', col='p_brand', dts=dts)} = 'Brand#23'
#     AND {self._json(tbl='p', col='p_container', dts=dts)} = 'MED BOX'
#     AND {self._json(tbl='l1', col='l_quantity', dts=dts)} < (
#         SELECT
#             0.2 * AVG({self._json(tbl='l2', col='l_quantity', dts=dts)})
#         FROM
#             extracted l2
#         WHERE
#             {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='l2', col='l_partkey', dts=dts)}
#     );
#     """

# REWRITE TO AVOID CROSS PRODUCT
        return f"""
, part_avg AS (
  SELECT
    {self._json(tbl='l2', col='l_partkey', dts=dts)}      AS partkey,
    AVG({self._json(tbl='l2', col='l_quantity', dts=dts)}) * 0.2 AS qty_threshold
  FROM extracted AS l2
  GROUP BY
    {self._json(tbl='l2', col='l_partkey', dts=dts)}
)

SELECT
    SUM({self._json(tbl='l1', col='l_extendedprice', dts=dts)}) / 7.0 AS avg_yearly
FROM extracted AS l1

  JOIN extracted AS p
    ON {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='l1', col='l_partkey', dts=dts)}

  JOIN part_avg AS pa
    ON pa.partkey = {self._json(tbl='l1', col='l_partkey', dts=dts)}

WHERE
    {self._json(tbl='p', col='p_brand', dts=dts)}     = 'Brand#23'
    AND {self._json(tbl='p', col='p_container', dts=dts)} = 'MED BOX'
    AND {self._json(tbl='l1', col='l_quantity', dts=dts)} < pa.qty_threshold;
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

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "p_partkey": True,
            "l_partkey": False,
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "p_brand": True,
            "p_container": True,
            "l_quantity": False
        }

        return field_map[field]
