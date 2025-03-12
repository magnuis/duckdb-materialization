SELECT 
    b.city AS city, 
    COUNT(r.review_id) / COUNT(DISTINCT r.business_id) AS avg_reviews_per_business, 
    COUNT(r.review_id) AS reviews, 
    COUNT(DISTINCT r.business_id) AS businesses
FROM test_view b, test_view r
WHERE r.business_id = b.business_id
    AND b.city IS NOT NULL
    AND r.review_id IS NOT NULL
GROUP BY b.city
ORDER BY avg_reviews_per_business DESC, city
-- ORDER BY avg_reviews_per_business DESC
LIMIT 100;
