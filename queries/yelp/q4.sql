SELECT COUNT(*) AS cnt, r.stars
FROM test_view r
WHERE r.review_id IS NOT NULL
GROUP BY r.stars
ORDER BY stars DESC;
