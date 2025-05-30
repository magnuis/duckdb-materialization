SELECT inReplyToUserIdStr AS user_id, COUNT(*) AS reply_count
FROM test_table
WHERE 
    inReplyToUserIdStr IS NOT NULL
GROUP BY inReplyToUserIdStr
ORDER BY reply_count DESC
LIMIT 15;
