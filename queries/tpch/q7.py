from queries.query import Query


class Q7(Query):
    """
    TPC-H Query 7
    """

    def __init__(self):
        super().__init__()

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 7, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM
    (
        SELECT
            {self._json(tbl='n1', col='n_name', dt=dts['n_name'])} AS supp_nation,
            {self._json(tbl='n2', col='n_name', dt=dts['n_name'])} AS cust_nation,
            EXTRACT(YEAR FROM {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])}) AS l_year,
            {self._json(tbl='l', col='l_extendedprice', dt=dts['l_extendedprice'])} * (1 - {self._json(tbl='l', col='l_discount', dt=dts['l_discount'])}) AS volume
        FROM
            test_table s,
            test_table l,
            test_table o,
            test_table c,
            test_table n1,
            test_table n2
        WHERE
            {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = {self._json(tbl='l', col='l_suppkey', dt=dts['l_suppkey'])}
            AND {self._json(tbl='o', col='o_orderkey', dt=dts['o_orderkey'])} = {self._json(tbl='l', col='l_orderkey', dt=dts['l_orderkey'])}
            AND {self._json(tbl='c', col='c_custkey', dt=dts['c_custkey'])} = {self._json(tbl='o', col='o_custkey', dt=dts['o_custkey'])}
            AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n1', col='n_nationkey', dt=dts['n_nationkey'])}
            AND {self._json(tbl='c', col='c_nationkey', dt=dts['c_nationkey'])} = {self._json(tbl='n2', col='n_nationkey', dt=dts['n_nationkey'])}
            AND (
                ({self._json(tbl='n1', col='n_name', dt=dts['n_name'])} = 'FRANCE' AND {self._json(tbl='n2', col='n_name', dt=dts['n_name'])} = 'GERMANY')
                OR ({self._json(tbl='n1', col='n_name', dt=dts['n_name'])} = 'GERMANY' AND {self._json(tbl='n2', col='n_name', dt=dts['n_name'])} = 'FRANCE')
            )
            AND {self._json(tbl='l', col='l_shipdate', dt=dts['l_shipdate'])} BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
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

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "n_name": True,
            "l_shipdate": True
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "s_suppkey": 1,
            "l_suppkey": 0,
            "o_orderkey": 1,
            "l_orderkey": 0,
            "c_custkey": 1,
            "o_custkey": 1,
            "s_nationkey": 1,
            "n_nationkey": 0,
            "c_nationkey": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
