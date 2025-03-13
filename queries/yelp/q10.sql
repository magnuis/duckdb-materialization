WITH cityUserStats (city, user_id, review_count, avg_stars) AS (
    SELECT 
        b.city,
        r.user_id,
        COUNT(*) AS review_count,
        AVG(r.stars) AS avg_stars
    FROM test_view b, test_view r
    WHERE 
        r.business_id = b.business_id
        AND b.city IS NOT NULL
    GROUP BY b.city, r.user_id
)
SELECT
    s.city,
    s.user_id,
    u.name AS username,
    s.review_count,
    s.avg_stars,
    (
        SELECT MAX(s2.review_count) 
        FROM cityUserStats s2 
        WHERE s2.city = s.city
    ) AS max_reviews_in_city,
    (
        SELECT MIN(s3.avg_stars) 
        FROM cityUserStats s3 
        WHERE s3.city = s.city
    ) AS min_avg_stars_in_city
FROM cityUserStats s, test_view u, 
    (
        SELECT 
            s1.city AS city, 
            s1.user_id AS top_user
        FROM cityUserStats s1
        WHERE s1.avg_stars = (
            SELECT MAX(s4.avg_stars) 
            FROM cityUserStats s4 
            WHERE s4.city = s1.city
        )
    ) topUser
WHERE 
    s.city = topUser.city
    AND s.user_id = topUser.top_user
    AND u.user_id = s.user_id
    AND s.review_count > 10
    AND u.name IS NOT NULL
ORDER BY 
    s.avg_stars DESC,
    s.city, 
    username
LIMIT 10;
