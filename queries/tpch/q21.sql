SELECT
    s.s_name,
    COUNT(*) AS numwait
FROM
    test_view s,
    test_view l1,
    test_view o,
    test_view n
WHERE
    s.s_suppkey = l1.l_suppkey
    AND o.o_orderkey = l1.l_orderkey
    AND o.o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            *
        FROM
            test_view l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            test_view l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s.s_nationkey = n.n_nationkey
    AND n.n_name = 'SAUDI ARABIA'
GROUP BY
    s.s_name
ORDER BY
    numwait DESC,
    s.s_name
LIMIT
    100;
