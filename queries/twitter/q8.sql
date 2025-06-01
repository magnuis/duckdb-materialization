SELECT 
    ANY_VALUE(user_info.user_screenName) AS screen_name,
    SUM(user_info.user_followersCount) AS followers_count,
    COUNT(DISTINCT reply.id_str) AS reply_count,
    COUNT(DISTINCT retweet.id_str) AS retweet_count
FROM 
    tweets user_info,
    (
        SELECT 
            tweets.idStr AS id_str,
            tweets.retweetedStatus_user_idStr AS retweeter
        FROM tweets 
    ) AS retweet,
    (
        SELECT 
            tweets.idStr AS id_str,
            tweets.inReplyToUserIdStr AS reply_to_user
        FROM tweets
    ) AS reply
WHERE 
    user_info.user_followersCount > 1000
    AND reply.reply_to_user = user_info.user_idStr
    AND retweet.retweeter = user_info.user_idStr
GROUP BY user_info.user_idStr
ORDER BY 
    followers_count DESC, 
    (reply_count + retweet_count) DESC
LIMIT 15;
