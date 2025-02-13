WITH revenue (supplier_no, total_revenue) AS (
    SELECT
        l.l_suppkey,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
    FROM
        test_view l
    WHERE
        l.l_shipdate >= DATE '1996-01-01'
        AND l.l_shipdate < DATE '1996-04-01'
    GROUP BY
        l.l_suppkey
)
SELECT
    s.s_suppkey,
    s.s_name,
    s.s_address,
    s.s_phone,
    total_revenue
FROM
    test_view s,
    revenue
WHERE
    s.s_suppkey = revenue.supplier_no
    AND total_revenue = (
        SELECT
            MAX(total_revenue)
        FROM
            revenue
    )
ORDER BY
    s.s_suppkey;
