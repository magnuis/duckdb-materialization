from queries.query import Query


class Q22(Query):
    """
    TPC-H Query 22
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 22, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM
    (
        SELECT
            SUBSTRING({self._json(tbl='c', col='c_phone', dt=dts['c_phone'])} FROM 1 FOR 2) AS cntrycode,
            {self._json(tbl='c', col='c_acctbal', dt=dts['c_acctbal'])} AS c_acctbal
        FROM
            test_table c
        WHERE
            SUBSTRING({self._json(tbl='c', col='c_phone', dt=dts['c_phone'])} FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            AND {self._json(tbl='c', col='c_acctbal', dt=dts['c_acctbal'])} > (
                SELECT
                    AVG({self._json(tbl='c', col='c_acctbal', dt=dts['c_acctbal'])})
                FROM
                    test_table c
                WHERE
                    {self._json(tbl='c', col='c_acctbal', dt=dts['c_acctbal'])} > 0.00
                    AND SUBSTRING({self._json(tbl='c', col='c_phone', dt=dts['c_phone'])} FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            )
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    test_table o
                WHERE
                    {self._json(tbl='o', col='o_custkey', dt=dts['o_custkey'])} = {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])}
            )
    ) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
    """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 22

        Returns
        -------
        list[str]
        """

        return [
            "c_phone",
            "c_acctbal",
            "c_custkey",
            "o_custkey"
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
                # underlying column for SUBSTRING(c_phone FROM 1 FOR 2)
                "c_phone",
                "c_acctbal"   # underlying column for c_acctbal
            ],
            'where': [
                # used in the IN condition on SUBSTRING(c_phone FROM 1 FOR 2)
                "c_phone",
                # used in the > condition comparing to AVG(c_acctbal)
                "c_acctbal"
            ],
            'group_by': [
                "c_phone"     # group by the computed country code derived from c_phone
            ],
            'order_by': [
                "c_phone"     # ordering by the computed country code
            ],
            'join': [
                "o_custkey",  # join condition in NOT EXISTS: o_custkey = c_custkey
                "c_custkey"   # join condition in NOT EXISTS: o_custkey = c_custkey
            ]
        }
