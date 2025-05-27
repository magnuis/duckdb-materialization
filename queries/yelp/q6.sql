SELECT
    COUNT(u.user_id) AS cnt, 
    u.user_id AS uid, 
    u.name, 
    AVG(u.average_stars) AS avg_stars,
    SUM(r.review_count) AS total_reviews,
    u.fans
FROM 
    test_view r,
    test_view u
WHERE
    r.user_id = u.user_id
    AND u.name IS NOT NULL
    AND u.average_stars IS NOT NULL
    AND r.review_id IS NOT NULL
GROUP BY u.user_id, u.name, u.fans
ORDER BY cnt DESC, u.fans
-- ORDER BY cnt DESC
LIMIT 10;
