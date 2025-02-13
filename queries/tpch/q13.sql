SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            c.c_custkey,
            COUNT(o.o_orderkey) AS c_count
        FROM
            test_view c LEFT OUTER JOIN test_view o ON
                c.c_custkey = o.o_custkey
                AND o.o_comment NOT LIKE '%special%requests%'
        GROUP BY
            c.c_custkey
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;
