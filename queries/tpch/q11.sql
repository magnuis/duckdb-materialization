SELECT
    ps.ps_partkey,
    SUM(ps.ps_supplycost * ps.ps_availqty) AS value
FROM
    test_view ps,
    test_view s,
    test_view n
WHERE
    ps.ps_suppkey = s.s_suppkey
    AND s.s_nationkey = n.n_nationkey
    AND n.n_name = 'GERMANY'
GROUP BY
    ps.ps_partkey
HAVING
    SUM(ps.ps_supplycost * ps.ps_availqty) > (
        SELECT
            SUM(ps.ps_supplycost * ps.ps_availqty) * 0.0001
        FROM
            test_view ps,
            test_view s,
            test_view n
        WHERE
            ps.ps_suppkey = s.s_suppkey
            AND s.s_nationkey = n.n_nationkey
            AND n.n_name = 'GERMANY'
    )
ORDER BY
    value DESC;
