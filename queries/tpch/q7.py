from queries.query import Query


class Q7(Query):
    """
    TPC-H Query 7
    """

    def __init__(self):
        pass

    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "c": ["c_custkey", "c_nationkey"],
            "l": ["l_shipdate", "l_extendedprice", "l_discount", "l_suppkey", "l_orderkey"],
            "o": ["o_custkey", "o_orderkey"],
            "n1": ["n_name", "n_nationkey"],
            "n2": ["n_name", "n_nationkey"],
            "s": ["s_suppkey", "s_nationkey"],
        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 7, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM
    (
        SELECT
            {self._json(tbl='n1', col='n_name', dts=dts)} AS supp_nation,
            {self._json(tbl='n2', col='n_name', dts=dts)} AS cust_nation,
            EXTRACT(YEAR FROM {self._json(tbl='l', col='l_shipdate', dts=dts)}) AS l_year,
            {self._json(tbl='l', col='l_extendedprice', dts=dts)} * (1 - {self._json(tbl='l', col='l_discount', dts=dts)}) AS volume
        FROM
            extracted s,
            extracted l,
            extracted o,
            extracted c,
            extracted n1,
            extracted n2
        WHERE
            {self._json(tbl='s', col='s_suppkey', dts=dts)} = {self._json(tbl='l', col='l_suppkey', dts=dts)}
            AND {self._json(tbl='o', col='o_orderkey', dts=dts)} = {self._json(tbl='l', col='l_orderkey', dts=dts)}
            AND {self._json(tbl='c', col='c_custkey', dts=dts)} = {self._json(tbl='o', col='o_custkey', dts=dts)}
            AND {self._json(tbl='s', col='s_nationkey', dts=dts)} = {self._json(tbl='n1', col='n_nationkey', dts=dts)}
            AND {self._json(tbl='c', col='c_nationkey', dts=dts)} = {self._json(tbl='n2', col='n_nationkey', dts=dts)}
            AND (
                ({self._json(tbl='n1', col='n_name', dts=dts)} = 'FRANCE' AND {self._json(tbl='n2', col='n_name', dts=dts)} = 'GERMANY')
                OR ({self._json(tbl='n1', col='n_name', dts=dts)} = 'GERMANY' AND {self._json(tbl='n2', col='n_name', dts=dts)} = 'FRANCE')
            )
            AND {self._json(tbl='l', col='l_shipdate', dts=dts)} BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    ) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;

    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 5

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
                "n_name",
                "n_name",
                "l_shipdate",
                "l_extendedprice",
                "l_discount"
            ],
            'where': [
                "n_name",
                "n_name",
                "l_shipdate"
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {
                "s_suppkey": ["l_suppkey"],
                "l_suppkey": ["s_suppkey"],
                "o_orderkey": ["l_orderkey"],
                "l_orderkey": ["o_orderkey"],
                "c_custkey": ["o_custkey"],
                "o_custkey": ["c_custkey"],
                "s_nationkey": ["n_nationkey"],
                "n_nationkey": ["s_nationkey", "c_nationkey"],
                "c_nationkey": ["n_nationkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "s_suppkey": False,
            "l_suppkey": True,
            "o_orderkey": False,
            "l_orderkey": True,
            "c_custkey": False,
            "o_custkey": False,
            "s_nationkey": False,
            "n_nationkey": True,
            "c_nationkey": False
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "n_name": True,
            "l_shipdate": True
        }

        return field_map[field]
