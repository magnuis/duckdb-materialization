SELECT 
    COUNT(DISTINCT r.user_id) AS cnt, 
    r.business_id AS bid
FROM test_view r
WHERE CAST(r.date AS DATE) > '2017-01-01'
    AND CAST(r.date AS DATE) < '2018-01-01'
    AND r.review_id IS NOT NULL
GROUP BY r.business_id
ORDER BY cnt DESC, bid
-- ORDER BY cnt DESC
LIMIT 10;
