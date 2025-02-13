SELECT
    s.s_acctbal,
    s.s_name,
    n.n_name,
    p.p_partkey,
    p.p_mfgr,
    s.s_address,
    s.s_phone,
    s.s_comment
FROM
    test_view p,
    test_view s,
    test_view ps,
    test_view n,
    test_view r
WHERE
    p.p_partkey = ps.ps_partkey
    AND s.s_suppkey = ps.ps_suppkey
    AND p.p_size = 15
    AND p.p_type LIKE '%BRASS'
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = 'EUROPE'
    AND ps.ps_supplycost = (
        SELECT
            MIN(ps.ps_supplycost)
        FROM
            test_view s,
            test_view ps,
            test_view n,
            test_view r
        WHERE
            p.p_partkey = ps.ps_partkey
            AND s.s_suppkey = ps.ps_suppkey
            AND s.s_nationkey = n.n_nationkey
            AND n.n_regionkey = r.r_regionkey
            AND r.r_name = 'EUROPE'
    )
ORDER BY
    s.s_acctbal DESC,
    n.n_name,
    s.s_name,
    p.p_partkey
LIMIT
    100;
