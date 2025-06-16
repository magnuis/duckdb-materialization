from queries.query import Query


class Q22(Query):
    """
    TPC-H Query 22
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
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
            SUBSTRING({self._json(tbl='c', col='c_phone', fields=fields)} FROM 1 FOR 2) AS cntrycode,
            {self._json(tbl='c', col='c_acctbal', fields=fields)} AS c_acctbal
        FROM
            test_table c
        WHERE
            SUBSTRING({self._json(tbl='c', col='c_phone', fields=fields)} FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            AND {self._json(tbl='c', col='c_acctbal', fields=fields)} > (
                SELECT
                    AVG({self._json(tbl='c', col='c_acctbal', fields=fields)})
                FROM
                    test_table c
                WHERE
                    {self._json(tbl='c', col='c_acctbal', fields=fields)} > 0.00
                    AND SUBSTRING({self._json(tbl='c', col='c_phone', fields=fields)} FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            )
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    test_table o
                WHERE
                    {self._json(tbl='o', col='o_custkey', fields=fields)} = {self._json(tbl='c', col='c_custkey', fields=fields)}
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

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "o_custkey": False,
            "c_custkey": True
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """

        field_map = {
            "c_phone": 2,
            "c_acctbal": 2
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "o_custkey": 1,
            "c_custkey": 0
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
