SELECT
    o_year,
    SUM(CASE
            WHEN nation = 'BRAZIL' THEN volume
            ELSE 0
        END) / SUM(volume) AS mkt_share
FROM
    (
        SELECT
            EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
            l.l_extendedprice * (1 - l.l_discount) AS volume,
            n2.n_name AS nation
        FROM
            test_view p,
            test_view s,
            test_view l,
            test_view o,
            test_view c,
            test_view n1,
            test_view n2,
            test_view r
        WHERE
            p.p_partkey = l.l_partkey
            AND s.s_suppkey = l.l_suppkey
            AND l.l_orderkey = o.o_orderkey
            AND o.o_custkey = c.c_custkey
            AND r.r_regionkey = n1.n_regionkey
            AND c.c_nationkey = n1.n_nationkey
            AND r.r_name = 'AMERICA'
            AND s.s_nationkey = n2.n_nationkey
            AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
            AND p.p_type = 'ECONOMY ANODIZED STEEL'
    ) AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year;
