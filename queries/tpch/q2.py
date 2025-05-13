from queries.query import Query


class Q2(Query):
    """
    TPC-H Query 2
    """

    def __init__(self):
        pass

#     def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
#         """
#         Get the formatted TPC-H query 2, adjusted to current db materializaiton

#         Returns
#         -------
#         str
#         """

#         dts = self._get_field_accesses(fields=fields)

#         return f"""
# SELECT
#     {self._json(tbl='s', col='s_acctbal', dts=dts)} AS s_acctbal,
#     {self._json(tbl='s', col='s_name', dts=dts)} AS s_name,
#     {self._json(tbl='n', col='n_name', dts=dts)} AS n_name,
#     {self._json(tbl='p', col='p_partkey', dts=dts)} AS p_partkey,
#     {self._json(tbl='p', col='p_mfgr', dts=dts)} AS p_mfgr,
#     {self._json(tbl='s', col='s_address', dts=dts)} AS s_address,
#     {self._json(tbl='s', col='s_phone', dts=dts)} AS s_phone,
#     {self._json(tbl='s', col='s_comment', dts=dts)} AS s_comment,
# FROM
#     test_table p,
#     test_table s,
#     test_table ps,
#     test_table n,
#     test_table r
# WHERE
#     {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='ps', col='ps_partkey', dts=dts)}
#     AND {self._json(tbl='s', col='s_suppkey', dts=dts)} = {self._json(tbl='ps', col='ps_suppkey', dts=dts)}
#     AND {self._json(tbl='p', col='p_size', dts=dts)} = 15
#     AND {self._json(tbl='p', col='p_type', dts=dts)}  LIKE '%BRASS'
#     AND {self._json(tbl='s', col='s_nationkey', dts=dts)} = {self._json(tbl='n', col='n_nationkey', dts=dts)}
#     AND {self._json(tbl='n', col='n_regionkey', dts=dts)} = {self._json(tbl='r', col='r_regionkey', dts=dts)}
#     AND {self._json(tbl='r', col='r_name', dts=dts)} = 'EUROPE'
#     AND {self._json(tbl='ps', col='ps_supplycost', dts=dts)} = (
#         SELECT
#             MIN({self._json(tbl='ps', col='ps_supplycost', dts=dts)})
#         FROM
#             test_table s,
#             test_table ps,
#             test_table n,
#             test_table r
#         WHERE
#             {self._json(tbl='p', col='p_partkey', dts=dts)} = {self._json(tbl='ps', col='ps_partkey', dts=dts)}
#             AND {self._json(tbl='s', col='s_suppkey', dts=dts)} = {self._json(tbl='ps', col='ps_suppkey', dts=dts)}
#             AND {self._json(tbl='s', col='s_nationkey', dts=dts)} = {self._json(tbl='n', col='n_nationkey', dts=dts)}
#             AND {self._json(tbl='n', col='n_regionkey', dts=dts)} = {self._json(tbl='r', col='r_regionkey', dts=dts)}
#             AND {self._json(tbl='r', col='r_name', dts=dts)} = 'EUROPE'
#     )
# ORDER BY
#     s_acctbal DESC,
#     n_name,
#     s_name,
#     p_partkey
# LIMIT
#     100;
#     """
    def get_cte_setups(self) -> str:
        """
        Rewrite the query using the recommended `WITH extraced AS` JSON syntax
        """

        return {
            "p1": ["p_partkey", "p_mfgr", "p_size", "p_type"],
            "ps1": ["ps_suppkey", "ps_supplycost", "ps_partkey"],
            "r1": ["r_name", "r_regionkey"],
            "n1": ["n_nationkey", "n_name", "n_regionkey"],
            "s1": ["s_nationkey", "s_suppkey"],
            "ps2": ["ps_supplycost", "ps_partkey", "ps_suppkey"],
            "n2": ["n_nationkey", "n_regionkey"],
            "r2": ["r_name", "r_regionkey"],
            "s2": ["s_name", "s_acctbal", "s_nationkey", "s_suppkey", "s_address", "s_phone", "s_comment"],

        }

    def _get_query(self, dts) -> str:
        """
        Get the formatted TPC-H query 2, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        return f"""
SELECT
        {self._json(tbl='s2', col='s_acctbal', dts=dts)} AS s_acctbal,
        {self._json(tbl='s2', col='s_name', dts=dts)} AS s_name,
        r_n_joined.n_name,
        p_ps_joined.p_partkey,
        p_ps_joined.p_mfgr,
        {self._json(tbl='s2', col='s_address', dts=dts)} AS s_address,
        {self._json(tbl='s2', col='s_phone', dts=dts)} AS s_phone,
        {self._json(tbl='s2', col='s_comment', dts=dts)} AS s_comment
FROM
        (
                SELECT
                        {self._json(tbl='ps1', col='ps_suppkey', dts=dts)} AS ps_suppkey,
                        {self._json(tbl='ps1', col='ps_supplycost', dts=dts)} AS ps_supplycost,
                        {self._json(tbl='p1', col='p_partkey', dts=dts)} AS p_partkey,
                        {self._json(tbl='p1', col='p_mfgr', dts=dts)} AS p_mfgr
                FROM
                        extracted p1,
                        extracted ps1
                WHERE
                        {self._json(tbl='p1', col='p_size', dts=dts)} = 15
                        AND {self._json(tbl='p1', col='p_type', dts=dts)} like '%BRASS'
                        AND {self._json(tbl='p1', col='p_partkey', dts=dts)} = {self._json(tbl='ps1', col='ps_partkey', dts=dts)}
        ) AS p_ps_joined,
        (
                SELECT
                        {self._json(tbl='n1', col='n_nationkey', dts=dts)} AS n_nationkey,
                        {self._json(tbl='n1', col='n_name', dts=dts)} AS n_name
                FROM
                        extracted r1,
                        extracted n1
                WHERE
                        {self._json(tbl='r1', col='r_name', dts=dts)} = 'EUROPE'
                        AND {self._json(tbl='n1', col='n_regionkey', dts=dts)} = {self._json(tbl='r1', col='r_regionkey', dts=dts)}
        ) AS r_n_joined,
        extracted s2
WHERE
        {self._json(tbl='s2', col='s_suppkey', dts=dts)} = p_ps_joined.ps_suppkey
        AND {self._json(tbl='s2', col='s_nationkey', dts=dts)} = r_n_joined.n_nationkey
        AND p_ps_joined.ps_supplycost = (
                SELECT
                        min({self._json(tbl='ps2', col='ps_supplycost',
                                        dts=dts)})
                FROM
                        extracted s1,
                        extracted ps2,
                        extracted n2,
                        extracted r2
                WHERE
                        p_ps_joined.p_partkey = {self._json(
                                            tbl='ps2', col='ps_partkey', dts=dts)}
                        AND {self._json(tbl='s1', col='s_suppkey', dts=dts)} = {self._json(tbl='ps2', col='ps_suppkey', dts=dts)}
                        AND {self._json(tbl='s1', col='s_nationkey', dts=dts)} = {self._json(tbl='n2', col='n_nationkey', dts=dts)}
                        AND {self._json(tbl='n2', col='n_regionkey', dts=dts)} = {self._json(tbl='r2', col='r_regionkey', dts=dts)}
                        AND {self._json(tbl='r2', col='r_name', dts=dts)} = 'EUROPE'
        )
ORDER BY
        {self._json(tbl='s2', col='s_acctbal', dts=dts)} DESC,
        r_n_joined.n_name,
        {self._json(tbl='s2', col='s_name', dts=dts)},
        p_ps_joined.p_partkey
LIMIT
        100;
        """

    def no_join_clauses(self) -> int:
        """
        Returns the number of join clauses in the query
        """
        return 4

    def columns_used_with_position(self,) -> dict[str, list[str]]:
        """
        Get the columns used in TPC-H Query 1 along with their position in the query 
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
                "s_acctbal",
                "s_name",
                "s_address",
                "s_phone",
                "s_comment",
                "ps_suppkey",
                "ps_supplycost",
                "p_partkey",
                "p_mfgr",
                "n_nationkey",
                "n_name",
                "ps_supplycost",
            ],
            'where': [
                "p_size",
                "p_type",
                "r_name",
                "r_name"
            ],
            'group_by': [],
            'order_by': [
                "s_acctbal",
                "s_name"
            ],
            'join': {
                "p_partkey": ["ps_partkey"],
                "ps_partkey": ["p_partkey", None],
                "n_regionkey": ["r_regionkey", "r_regionkey"],
                "r_regionkey": ["n_regionkey", "n_regionkey"],
                "s_suppkey": [None, "ps_suppkey"],
                "s_nationkey": [None, "n_nationkey"],
                "ps_suppkey": ["s_suppkey"],
                "n_nationkey": ["s_nationkey"]
            }
        }
