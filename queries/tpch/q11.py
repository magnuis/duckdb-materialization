from queries.query import Query


class Q11(Query):
    """
    TPC-H Query 11
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "ps1": ["ps_supplycost", "ps_availqty", "ps_suppkey"],
            "s1": ["s_nationkey", "s_suppkey"],
            "n1": ["n_nationkey", "n_name"],
            "ps2": ["ps_supplycost", "ps_availqty", "ps_suppkey", "ps_partkey"],
            "s2": ["s_nationkey", "s_suppkey"],
            "n2": ["n_nationkey", "n_name"],
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 11, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='ps2', col='ps_partkey', dts=dts)} AS ps_partkey,
    SUM({self._json(tbl='ps2', col='ps_supplycost', dts=dts)} * {self._json(tbl='ps2', col='ps_availqty', dts=dts)}) AS value
FROM
    extracted ps2,
    extracted s2,
    extracted n2
WHERE
    {self._json(tbl='ps2', col='ps_suppkey', dts=dts)} = {self._json(tbl='s2', col='s_suppkey', dts=dts)}
    AND {self._json(tbl='s2', col='s_nationkey', dts=dts)} = {self._json(tbl='n2', col='n_nationkey', dts=dts)}
    AND {self._json(tbl='n2', col='n_name', dts=dts)} = 'GERMANY'
GROUP BY
    {self._json(tbl='ps2', col='ps_partkey', dts=dts)}
HAVING
    SUM({self._json(tbl='ps2', col='ps_supplycost', dts=dts)} * {self._json(tbl='ps2', col='ps_availqty', dts=dts)}) > (
        SELECT
            SUM({self._json(tbl='ps1', col='ps_supplycost', dts=dts)} * {self._json(tbl='ps1', col='ps_availqty', dts=dts)}) * 0.0001
        FROM
            extracted ps1,
            extracted s1,
            extracted n1
        WHERE
            {self._json(tbl='ps1', col='ps_suppkey', dts=dts)} = {self._json(tbl='s1', col='s_suppkey', dts=dts)}
            AND {self._json(tbl='s1', col='s_nationkey', dts=dts)} = {self._json(tbl='n1', col='n_nationkey', dts=dts)}
            AND {self._json(tbl='n1', col='n_name', dts=dts)} = 'GERMANY'
    )
ORDER BY
    value DESC,
    ps_partkey;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 4

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
                "ps_partkey",
                "ps_supplycost",
                "ps_availqty",
                "ps_supplycost",
                "ps_availqty"
            ],
            'where': [
                "ps_supplycost",
                "ps_availqty",
                "n_name",
                "n_name"
            ],
            'group_by': [
                "ps_partkey"
            ],
            'order_by': [
            ],
            'join': {
                "ps_suppkey": ["s_suppkey", "s_suppkey"],
                "s_suppkey": ["ps_suppkey", "ps_suppkey"],
                "s_nationkey": ["n_nationkey", "n_nationkey"],
                "n_nationkey": ["s_nationkey", "s_nationkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "ps_suppkey": False,
            "s_suppkey": False,
            "s_nationkey": False,
            "n_nationkey": True,
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "ps_supplycost": False,
            "ps_availqty": False,
            "n_name": True,
        }

        return field_map[field]
