from queries.query import Query


class Q22(Query):
    """
    TPC-H Query 22
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "c1": ["c_acctbal", "c_phone"],
            "c2": ["c_acctbal", "c_phone", "c_custkey"],
            "o": ["o_custkey"],
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 22, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM
    (
        SELECT
            SUBSTRING({self._json(tbl='c2', col='c_phone', dts=dts)} FROM 1 FOR 2) AS cntrycode,
            {self._json(tbl='c2', col='c_acctbal', dts=dts)} AS c_acctbal
        FROM
            extracted c2
        WHERE
            SUBSTRING({self._json(tbl='c2', col='c_phone', dts=dts)} FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            AND {self._json(tbl='c2', col='c_acctbal', dts=dts)} > (
                SELECT
                    AVG({self._json(tbl='c1', col='c_acctbal', dts=dts)})
                FROM
                    extracted c1
                WHERE
                    {self._json(tbl='c1', col='c_acctbal', dts=dts)} > 0.00
                    AND SUBSTRING({self._json(tbl='c1', col='c_phone', dts=dts)} FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            )
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    extracted o
                WHERE
                    {self._json(tbl='o', col='o_custkey', dts=dts)} = {self._json(tbl='c2', col='c_custkey', dts=dts)}
            )
    ) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
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
                "c_phone",
                "c_acctbal",
                "c_acctbal"

            ],
            'where': [
                "c_phone",
                "c_acctbal",
                "c_acctbal",
                "c_phone"
            ],
            'group_by': [
                "c_phone"
            ],
            'order_by': [
                "c_phone"
            ],
            'join': {
                "o_custkey": ["c_custkey"],
                "c_custkey": ["o_custkey"]
            }
        }
