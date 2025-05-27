SELECT 
    city,
    state,
    COUNT(*) AS total_businesses,
    AVG(stars) AS avg_stars,
    AVG(review_count) AS avg_reviews,
    MIN(stars) AS min_stars,
    MAX(stars) AS max_stars
FROM test_view
GROUP BY city, state
ORDER BY avg_stars, avg_reviews, state, city DESC;
