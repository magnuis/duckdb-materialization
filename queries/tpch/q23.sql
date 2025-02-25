SELECT l.raw_json FROM test_view l
WHERE
    l.l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
ORDER BY
    l.l_partkey,
    l.l_orderkey
LIMIT 10;