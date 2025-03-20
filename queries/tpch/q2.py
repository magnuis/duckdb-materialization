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

#         # TODO use more performant e.g. dict for loopup

#         dts = self._get_field_accesses(fields=fields)

#         return f"""
# SELECT
#     {self._json(tbl='s', col='s_acctbal', dt=dts['s_acctbal'])} AS s_acctbal,
#     {self._json(tbl='s', col='s_name', dt=dts['s_name'])} AS s_name,
#     {self._json(tbl='n', col='n_name', dt=dts['n_name'])} AS n_name,
#     {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} AS p_partkey,
#     {self._json(tbl='p', col='p_mfgr', dt=dts['p_mfgr'])} AS p_mfgr,
#     {self._json(tbl='s', col='s_address', dt=dts['s_address'])} AS s_address,
#     {self._json(tbl='s', col='s_phone', dt=dts['s_phone'])} AS s_phone,
#     {self._json(tbl='s', col='s_comment', dt=dts['s_comment'])} AS s_comment,
# FROM
#     test_table p,
#     test_table s,
#     test_table ps,
#     test_table n,
#     test_table r
# WHERE
#     {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])}
#     AND {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])}
#     AND {self._json(tbl='p', col='p_size', dt=dts['p_size'])} = 15
#     AND {self._json(tbl='p', col='p_type', dt=dts['p_type'])}  LIKE '%BRASS'
#     AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
#     AND {self._json(tbl='n', col='n_regionkey', dt=dts['n_regionkey'])} = {self._json(tbl='r', col='r_regionkey', dt=dts['r_regionkey'])}
#     AND {self._json(tbl='r', col='r_name', dt=dts['r_name'])} = 'EUROPE'
#     AND {self._json(tbl='ps', col='ps_supplycost', dt=dts['ps_supplycost'])} = (
#         SELECT
#             MIN({self._json(tbl='ps', col='ps_supplycost', dt=dts['ps_supplycost'])})
#         FROM
#             test_table s,
#             test_table ps,
#             test_table n,
#             test_table r
#         WHERE
#             {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])}
#             AND {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])}
#             AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
#             AND {self._json(tbl='n', col='n_regionkey', dt=dts['n_regionkey'])} = {self._json(tbl='r', col='r_regionkey', dt=dts['r_regionkey'])}
#             AND {self._json(tbl='r', col='r_name', dt=dts['r_name'])} = 'EUROPE'
#     )
# ORDER BY
#     s_acctbal DESC,
#     n_name,
#     s_name,
#     p_partkey
# LIMIT
#     100;
#     """


class Q2(Query):
    """
    TPC-H Query 2
    """

    def __init__(self):
        pass

    def get_query(self, fields: list[tuple[str, dict, bool]]) -> str:
        """
        Get the formatted TPC-H query 2, adjusted to current db materializaiton

        Returns
        -------
        str
        """

        # TODO use more performant e.g. dict for loopup

        dts = self._get_field_accesses(fields=fields)

        return f"""
SELECT
        {self._json(tbl='s', col='s_acctbal', dt=dts['s_acctbal'])} AS s_acctbal,
        {self._json(tbl='s', col='s_name', dt=dts['s_name'])} AS s_name,
        r_n_joined.n_name,
        p_ps_joined.p_partkey,
        p_ps_joined.p_mfgr,
        {self._json(tbl='s', col='s_address', dt=dts['s_address'])} AS s_address,
        {self._json(tbl='s', col='s_phone', dt=dts['s_phone'])} AS s_phone,
        {self._json(tbl='s', col='s_comment', dt=dts['s_comment'])} AS s_comment
FROM
        (
                SELECT
                        {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])} AS ps_suppkey,
                        {self._json(tbl='ps', col='ps_supplycost', dt=dts['ps_supplycost'])} AS ps_supplycost,
                        {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} AS p_partkey,
                        {self._json(tbl='p', col='p_mfgr', dt=dts['p_mfgr'])} AS p_mfgr
                FROM
                        test_table p,
                        test_table ps
                WHERE
                        {self._json(tbl='p', col='p_size', dt=dts['p_size'])} = 15
                        AND {self._json(tbl='p', col='p_type', dt=dts['p_type'])} like '%BRASS'
                        AND {self._json(tbl='p', col='p_partkey', dt=dts['p_partkey'])} = {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])}
        ) AS p_ps_joined,
        (
                SELECT
                        {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])} AS n_nationkey,
                        {self._json(tbl='r', col='r_name', dt=dts['r_name'])} AS n_name
                FROM
                        test_table r,
                        test_table n
                WHERE
                        {self._json(tbl='r', col='r_name', dt=dts['r_name'])} = 'EUROPE'
                        AND {self._json(tbl='n', col='n_regionkey', dt=dts['n_regionkey'])} = {self._json(tbl='r', col='r_regionkey', dt=dts['r_regionkey'])}
        ) AS r_n_joined,
        test_table s
WHERE
        
        {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = p_ps_joined.ps_suppkey
        AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = r_n_joined.n_nationkey
        AND p_ps_joined.ps_supplycost = (
                SELECT
                        min({self._json(tbl='ps', col='ps_supplycost', dt=dts['ps_supplycost'])})
                FROM
                        test_table s,
                        test_table ps,
                        test_table n,
                        test_table r
                WHERE
                        p_ps_joined.p_partkey = {self._json(tbl='ps', col='ps_partkey', dt=dts['ps_partkey'])}
                        AND {self._json(tbl='s', col='s_suppkey', dt=dts['s_suppkey'])} = {self._json(tbl='ps', col='ps_suppkey', dt=dts['ps_suppkey'])}
                        AND {self._json(tbl='s', col='s_nationkey', dt=dts['s_nationkey'])} = {self._json(tbl='n', col='n_nationkey', dt=dts['n_nationkey'])}
                        AND {self._json(tbl='n', col='n_regionkey', dt=dts['n_regionkey'])} = {self._json(tbl='r', col='r_regionkey', dt=dts['r_regionkey'])}
                        AND {self._json(tbl='r', col='r_name', dt=dts['r_name'])} = 'EUROPE'

        )
ORDER BY
        {self._json(tbl='s', col='s_acctbal', dt=dts['s_acctbal'])} DESC,
        r_n_joined.n_name,
        {self._json(tbl='s', col='s_name', dt=dts['s_name'])},
        p_ps_joined.p_partkey
LIMIT
        100;
        """

    def columns_used(self,) -> list[str]:
        """
        Get the columns used in TPC-H query 2

        Returns
        -------
        list[str]
        """

        return [
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
            "ps_partkey",
            "ps_suppkey",
            "s_suppkey",
            "p_size",
            "p_type",
            "s_nationkey",
            "n_nationkey",
            "n_regionkey",
            "r_regionkey",
            "r_name",
            "n_name",
            "ps_supplycost",
            "s_acctbal"
        ]
