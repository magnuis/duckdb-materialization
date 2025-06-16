from queries.query import Query


class Q9(Query):
    """
    TPC-H Query 9
    """

    def __init__(self, dataset: str):
        super().__init__(dataset=dataset)

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 9, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
    SELECT
            nation,
            o_year,
            SUM(amount) AS sum_profit
    FROM
            (
                    SELECT
                            {self._json(tbl='n', col='n_name', fields=fields)}  AS nation,
                            EXTRACT(YEAR FROM {self._json(tbl='o', col='o_orderdate', fields=fields)})  AS o_year,
                            lp_joined.l_extendedprice * (1 - lp_joined.l_discount) - {self._json(tbl='ps', col='ps_supplycost', fields=fields)} * lp_joined.l_discount AS amount
                    FROM
                            (
                                    SELECT
                                            {self._json(tbl='l', col='l_partkey', fields=fields)} AS l_partkey,
                                            {self._json(tbl='l', col='l_suppkey', fields=fields)} AS l_suppkey,
                                            {self._json(tbl='l', col='l_orderkey', fields=fields)} AS l_orderkey,
                                            {self._json(tbl='l', col='l_extendedprice', fields=fields)} AS l_extendedprice,
                                            {self._json(tbl='l', col='l_discount', fields=fields)} AS l_discount

                                    FROM
                                            test_table p,
                                            test_table l
                                    WHERE
                                            {self._json(tbl='p', col='p_name', fields=fields)} like '%green%'
                                            AND {self._json(tbl='p', col='p_partkey', fields=fields)} = {self._json(tbl='l', col='l_partkey', fields=fields)}
                            ) AS lp_joined,
                            test_table s,
                            test_table ps,
                            test_table o,
                            test_table n
                    WHERE
                            {self._json(tbl='s', col='s_suppkey', fields=fields)} = lp_joined.l_suppkey
                            AND {self._json(tbl='ps', col='ps_suppkey', fields=fields)} = lp_joined.l_suppkey
                            AND {self._json(tbl='ps', col='ps_partkey', fields=fields)} = lp_joined.l_partkey
                            AND {self._json(tbl='o', col='o_orderkey', fields=fields)} = lp_joined.l_orderkey
                            AND {self._json(tbl='s', col='s_nationkey', fields=fields)} = {self._json(tbl='n', col='n_nationkey', fields=fields)}
            ) AS profit
    GROUP BY
            nation,
            o_year
    ORDER BY
            nation,
            o_year desc;

    """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 6

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
                "n_name",
                "o_orderdate",
                "ps_supplycost",
                "l_partkey",
                "l_suppkey",
                "l_orderkey",
                "l_extendedprice",
                "l_discount"
            ],
            'where': [
                "p_name"
            ],
            'group_by': [
            ],
            'order_by': [
            ],
            'join': {
                "p_partkey": ["l_partkey"],
                "l_partkey": ["p_partkey"],
                "s_suppkey": [None],
                "ps_suppkey": [None],
                "ps_partkey": [None],
                "s_nationkey": ["n_nationkey"],
                "n_nationkey": ["s_nationkey"],
                "o_orderkey": [None]
            }
        }

    def get_join_field_has_filter(self, field: str) -> str | None:
        """
        Query specific implementation of the join field filter
        """

        field_map = {
            "p_partkey": True,
            "l_partkey": False,
            "s_suppkey": False,
            "ps_suppkey": False,
            "ps_partkey": False,
            "s_nationkey": False,
            "n_nationkey": False,
            "o_orderkey": False
        }

        return field_map.get(field, False)

    def get_where_field_has_direct_filter(self, field: str, prev_materialization: list[str]) -> int:
        """
        Query specific implementation of the where field has direct filter
        """
        field_map = {
            "p_name": 1
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
            "s_suppkey": 1,
            "ps_suppkey": 1,
            "ps_partkey": 1,
            "s_nationkey": 1,
            "n_nationkey": 1,
            "o_orderkey": 1
        }

        if field not in field_map:
            raise ValueError(f"{field} not a JOIN field")

        return field_map[field]
