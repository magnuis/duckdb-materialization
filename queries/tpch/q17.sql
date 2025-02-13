SELECT
    SUM(l.l_extendedprice) / 7.0 AS avg_yearly
FROM
    test_view l,
    test_view p
WHERE
    p.p_partkey = l.l_partkey
    AND p.p_brand = 'Brand#23'
    AND p.p_container = 'MED BOX'
    AND l.l_quantity < (
        SELECT
            0.2 * AVG(l.l_quantity)
        FROM
            test_view l
        WHERE
            p.p_partkey = l.l_partkey
    );
