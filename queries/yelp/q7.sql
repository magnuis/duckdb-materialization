SELECT 
    r1.user_id,
    r1.review_id AS review_id_1,
    r1.business_id AS business_id_1,
    r1.stars AS stars_1,
    r2.review_id AS review_id_2,
    r2.business_id AS business_id_2,
    r2.stars AS stars_2,
    (r1.stars - r2.stars) AS stars_difference
FROM test_view r1
JOIN test_view r2
    ON r1.user_id = r2.user_id 
    AND r1.review_id <> r2.review_id
WHERE r1.date < r2.date
ORDER BY r1.user_id, r1.date;
