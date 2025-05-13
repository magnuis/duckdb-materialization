from queries.query import Query


class Q13(Query):
    """
    TPC-H Query 13
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "c": ["c_custkey"],
            "o": ["o_comment", "o_custkey", "o_orderkey"]
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 13, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            {self._json(tbl='c', col='c_custkey', dts=dts)} AS c_custkey,
            COUNT({self._json(tbl='o', col='o_orderkey', dts=dts)}) AS c_count
        FROM
            extracted c LEFT OUTER JOIN extracted o ON
                {self._json(tbl='c', col='c_custkey', dts=dts)} = {self._json(tbl='o', col='o_custkey', dts=dts)}
                AND {self._json(tbl='o', col='o_comment', dts=dts)} NOT LIKE '%special%requests%'
        GROUP BY
            {self._json(tbl='c', col='c_custkey', dts=dts)}
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;
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
                "c_custkey",
                "o_orderkey"
            ],
            'where': [
                "o_comment"
            ],
            'group_by': [
                "c_custkey"
            ],
            'order_by': [
            ],
            'join': {
                "c_custkey": ["o_custkey"],
                "o_custkey": ["c_custkey"]
            }
        }
