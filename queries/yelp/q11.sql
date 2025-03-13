SELECT 
    b.city,
    COUNT(DISTINCT b.business_id) AS total_businesses,
    COUNT(r.review_id) AS total_reviews,
    AVG(r.stars) AS avg_stars
FROM test_view b, test_view r
WHERE b.business_id = r.business_id
  AND b.city IS NOT NULL
GROUP BY b.city
ORDER BY total_reviews DESC
LIMIT 10;
