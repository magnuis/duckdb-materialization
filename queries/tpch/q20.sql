SELECT
    s.s_name,
    s.s_address
FROM
    test_view s,
    test_view n
WHERE
    s.s_suppkey IN (
        SELECT
            ps.ps_suppkey
        FROM
            test_view ps
        WHERE
            ps.ps_partkey IN (
                SELECT
                    p.p_partkey
                FROM
                    test_view p
                WHERE
                    p.p_name LIKE 'forest%'
            )
            AND ps.ps_availqty > (
                SELECT
                    0.5 * SUM(l.l_quantity)
                FROM
                    test_view l
                WHERE
                    l.l_partkey = ps.ps_partkey
                    AND l.l_suppkey = ps.ps_suppkey
                    AND l.l_shipdate >= DATE '1994-01-01'
                    AND l.l_shipdate < DATE '1995-01-01'
            )
    )
    AND s.s_nationkey = n.n_nationkey
    AND n.n_name = 'CANADA'
ORDER BY
    s.s_name;
