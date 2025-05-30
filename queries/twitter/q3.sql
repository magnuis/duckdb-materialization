SELECT 
    retweetedStatus_user_screenName AS user, 
    SUM(retweetedStatus_retweetCount) AS total_retweets
FROM test_table t
WHERE retweetedStatus_idStr IS NOT NULL
GROUP BY retweetedStatus_user_screenName
ORDER BY total_retweets DESC
LIMIT 10;
