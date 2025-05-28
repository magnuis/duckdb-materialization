from queries.query import Query


class Q18(Query):
    """
    TPC-H Query 18
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "c": ["c_name", "c_custkey"],
            "o": ["o_orderkey", "o_orderdate", "o_totalprice", "o_custkey"],
            "l1": ["l_quantity", "l_orderkey"],
            "l2": ["l_quantity", "l_orderkey"],
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 18, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    {self._json(tbl='c', col='c_name', dts=dts)} AS c_name,
    {self._json(tbl='c', col='c_custkey', dts=dts)} AS c_custkey,
    {self._json(tbl='o', col='o_orderkey', dts=dts)} AS o_orderkey,
    {self._json(tbl='o', col='o_orderdate', dts=dts)} AS o_orderdate,
    {self._json(tbl='o', col='o_totalprice', dts=dts)} AS o_totalprice,
    SUM({self._json(tbl='l2', col='l_quantity', dts=dts)}) AS total_quantity
FROM
    extracted c,
    extracted o,
    extracted l2
WHERE
    {self._json(tbl='o', col='o_orderkey', dts=dts)} IN (
        SELECT
            {self._json(tbl='l1', col='l_orderkey', dts=dts)}
        FROM
            extracted l1
        GROUP BY
            {self._json(tbl='l1', col='l_orderkey', dts=dts)}
        HAVING
            SUM({self._json(tbl='l1', col='l_quantity', dts=dts)}) > 300
    )
    AND {self._json(tbl='c', col='c_custkey', dts=dts)} = {self._json(tbl='o', col='o_custkey', dts=dts)}
    AND {self._json(tbl='o', col='o_orderkey', dts=dts)} = {self._json(tbl='l2', col='l_orderkey', dts=dts)}
GROUP BY
    {self._json(tbl='c', col='c_name', dts=dts)},
    {self._json(tbl='c', col='c_custkey', dts=dts)},
    {self._json(tbl='o', col='o_orderkey', dts=dts)},
    {self._json(tbl='o', col='o_orderdate', dts=dts)},
    {self._json(tbl='o', col='o_totalprice', dts=dts)}
ORDER BY
    {self._json(tbl='o', col='o_totalprice', dts=dts)} DESC,
    {self._json(tbl='o', col='o_orderdate', dts=dts)}
LIMIT
    100;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 2

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
                "c_name",
                "c_custkey",
                "o_orderkey",
                "o_orderdate",
                "o_totalprice",
                "l_quantity",
                "l_orderkey",
                "l_quantity"  # Fra HAVING
            ],
            'where': [
                "o_orderkey",
                "l_quantity"
            ],
            'group_by': [
                "l_orderkey"
                "c_name",
                "c_custkey",
                "o_orderkey",
                "o_orderdate",
                "o_totalprice"
            ],
            'order_by': [
                "o_totalprice",
                "o_orderdate"
            ],
            'join': {
                "c_custkey": ["o_custkey"],
                "o_custkey": ["c_custkey"],
                "l_orderkey": ["o_orderkey"],
                "o_orderkey": ["l_orderkey"]
            },

        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "c_custkey": False,
            "o_custkey": False,
            "l_orderkey": False,
            "o_orderkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "o_orderkey": False,
            "l_quantity": False
        }

        return field_map[field]
