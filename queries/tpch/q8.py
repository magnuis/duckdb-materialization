from queries.query import Query


class Q8(Query):
    """
    TPC-H Query 8
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 8, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
        o_year,
        SUM(case
                WHEN nation = 'BRAZIL' THEN volume
                ELSE 0
        END) / SUM(volume) AS mkt_share
FROM
        (
                SELECT
                        extract(year FROM {self._json(tbl='o', col='o_orderdate', fields=fields)}) AS o_year,
                        p_l_joined.l_extendedprice * (1  - p_l_joined.l_discount) AS volume,
                        {self._json(tbl='n2', col='n_name', fields=fields)} AS nation
                FROM
                        (
                                SELECT
                                        {self._json(tbl='p', col='p_partkey', fields=fields)} AS p_partkey,
                                        {self._json(tbl='l', col='l_extendedprice', fields=fields)} AS l_extendedprice,
                                        {self._json(tbl='l', col='l_discount', fields=fields)} AS l_discount,
                                        {self._json(tbl='l', col='l_orderkey', fields=fields)} AS l_orderkey,
                                        {self._json(tbl='l', col='l_partkey', fields=fields)} AS l_partkey,
                                        {self._json(tbl='l', col='l_suppkey', fields=fields)} AS l_suppkey
                                FROM 
                                        test_table p,
                                        test_table l
                                WHERE
                                        {self._json(tbl='p', col='p_type', fields=fields)} = 'ECONOMY ANODIZED STEEL'
                                        AND {self._json(tbl='p', col='p_partkey', fields=fields)} = {self._json(tbl='l', col='l_partkey', fields=fields)}

                        ) AS p_l_joined,
                        (
                                SELECT
                                        {self._json(tbl='n1', col='n_nationkey', fields=fields)} AS n_nationkey
                                FROM
                                        test_table r,
                                        test_table n1
                                WHERE
                                        {self._json(tbl='r', col='r_name', fields=fields)} = 'AMERICA'
                                        AND {self._json(tbl='r', col='r_regionkey', fields=fields)} = {self._json(tbl='n1', col='n_regionkey', fields=fields)}
                        ) AS r_n1_joined,

                        test_table o,
                        test_table c,
                        test_table s,
                        test_table n2
                WHERE

                        {self._json(tbl='s', col='s_suppkey', fields=fields)} = p_l_joined.l_suppkey
                        AND p_l_joined.l_orderkey = {self._json(tbl='o', col='o_orderkey', fields=fields)}
                        AND {self._json(tbl='o', col='o_custkey', fields=fields)} = {self._json(tbl='c', col='c_custkey', fields=fields)}
                        AND {self._json(tbl='c', col='c_nationkey', fields=fields)} = r_n1_joined.n_nationkey
                        AND {self._json(tbl='s', col='s_nationkey', fields=fields)} = {self._json(tbl='n2', col='n_nationkey', fields=fields)}
                        AND {self._json(tbl='o', col='o_orderdate', fields=fields)} BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        ) AS all_nations
GROUP BY
        o_year
ORDER BY
        o_year;
    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 7

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
                "o_orderdate",
                "n_name",
                "p_partkey",
                "l_extendedprice",
                "l_discount",
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "n_nationkey"
            ],
            'where': [
                "p_type",
                "r_name",
                "o_orderdate",
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {
                "p_partkey": ["l_partkey"],
                "l_partkey": ["p_partkey"],
                "r_regionkey": ["n_regionkey"],
                "n_regionkey": ["r_regionkey"],
                "s_suppkey": [None],
                "o_orderkey": [None],
                "o_custkey": ["c_custkey"],
                "c_custkey": ["o_custkey"],
                "c_nationkey": [None],
                "s_nationkey": ["n_nationkey"],
                "n_nationkey": ["s_nationkey"]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "p_partkey": True,
            "l_partkey": False,
            "r_regionkey": True,
            "n_regionkey": False,
            "s_suppkey": False,
            "o_orderkey": True,
            "o_custkey": True,
            "c_custkey": False,
            "c_nationkey": False,
            "s_nationkey": False,
            "n_nationkey": False
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "p_type": 1,
            "r_name": 1,
            "o_orderdate": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a WHERE field")
        return field_map[field]

    def get_join_field_has_no_direct_filter(self, field: str) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "p_partkey": 0,
            "l_partkey": 1,
            "r_regionkey": 0,
            "n_regionkey": 1,
            "s_suppkey": 1,
            "o_orderkey": 0,
            "o_custkey": 0,
            "c_custkey": 1,
            "c_nationkey": 1,
            "s_nationkey": 1,
            "n_nationkey": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
