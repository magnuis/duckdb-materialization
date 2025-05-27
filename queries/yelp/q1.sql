SELECT 
    COUNT(u.user_id) AS cnt, 
    u.user_id AS uid, 
    u.name, 
    u.average_stars AS avg_stars
FROM 
    test_view u, 
    test_view r
WHERE 
    r.user_id = u.user_id
    AND u.name IS NOT NULL
    AND u.average_stars IS NOT NULL
    AND r.review_id IS NOT NULL
GROUP BY u.user_id, u.name, u.average_stars
ORDER BY cnt DESC, u.name
-- ORDER BY cnt DESC
LIMIT 10;
