SELECT 
    b1.city,
    b1.business_id AS business_id1,
    b1.name AS business_name1,
    b2.business_id AS business_id2,
    b2.name AS business_name2,
    ABS(b1.stars - b2.stars) AS star_diff,
    ABS(b1.review_count - b2.review_count) AS review_count_diff
FROM test_view AS b1
JOIN test_view AS b2 
    ON b1.city = b2.city 
    AND b1.business_id < b2.business_id
WHERE 
    b1.is_open = 1 
    AND b2.is_open = 1
ORDER BY star_diff DESC
LIMIT 100;
