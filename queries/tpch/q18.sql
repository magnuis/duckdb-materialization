SELECT
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    SUM(l.l_quantity) AS total_quantity
FROM
    test_view c,
    test_view o,
    test_view l
WHERE
    o.o_orderkey IN (
        SELECT
            l.l_orderkey
        FROM
            test_view l
        GROUP BY
            l.l_orderkey
        HAVING
            SUM(l.l_quantity) > 300
    )
    AND c.c_custkey = o.o_custkey
    AND o.o_orderkey = l.l_orderkey
GROUP BY
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice
ORDER BY
    o.o_totalprice DESC,
    o.o_orderdate
LIMIT
    100;
