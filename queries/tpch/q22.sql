SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM
    (
        SELECT
            SUBSTRING(c.c_phone FROM 1 FOR 2) AS cntrycode,
            c.c_acctbal
        FROM
            test_view c
        WHERE
            SUBSTRING(c.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            AND c.c_acctbal > (
                SELECT
                    AVG(c.c_acctbal)
                FROM
                    test_view c
                WHERE
                    c.c_acctbal > 0.00
                    AND SUBSTRING(c.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            )
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    test_view o
                WHERE
                    o.o_custkey = c.c_custkey
            )
    ) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
