WITH citycount (city, cnt, userid) AS (
    SELECT DISTINCT b.city AS city, COUNT(*) AS cnt, r.user_id AS userid
    FROM test_view b, test_view r
    WHERE 
        r.business_id = b.business_id
        AND b.city IS NOT NULL
        AND r.review_id IS NOT NULL
    GROUP BY b.city, r.user_id
)
SELECT
    c.city AS city, 
    c.userid AS userid, 
    u.name AS username, 
    c.cnt AS review_cnt
FROM 
    citycount c, 
    test_view u, 
    (
        SELECT 
            c2.userid AS userid, 
            c2.city AS city 
        FROM citycount c2 
        WHERE c2.cnt = (
            SELECT MAX(c3.cnt) 
            FROM citycount c3 
            WHERE c2.city = c3.city
        )
    ) cj
WHERE 
    u.name IS NOT NULL
    AND u.average_stars IS NOT NULL
    AND c.userid = cj.userid
    AND c.city = cj.city
    AND c.userid = u.user_id
ORDER BY c.cnt DESC, city, username
LIMIT 10;
