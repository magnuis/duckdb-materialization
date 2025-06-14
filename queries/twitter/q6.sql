SELECT 
    initial_tweet.idStr AS initial_tweet_id,
    initial_tweet.retweetedStatus_user_screenName AS initial_author,
    retweet1.retweetedStatus_user_screenName AS first_retweeter,
    retweet2.retweetedStatus_user_screenName AS second_retweeter
FROM tweets AS initial_tweet
JOIN tweets AS retweet1 
    ON retweet1.retweetedStatus_idStr = initial_tweet.idStr
JOIN tweets AS retweet2 
    ON retweet2.retweetedStatus_idStr = retweet1.idStr
ORDER BY initial_tweet_id
LIMIT 20;
