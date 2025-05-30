SELECT source, COUNT(*) AS tweet_count
FROM test_table
GROUP BY source
ORDER BY tweet_count DESC;
