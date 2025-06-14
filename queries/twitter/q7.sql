SELECT 
    original_tweet.idStr AS original_tweet_id,
    original_tweet.user_screenName AS original_author,
    retweet.user_screenName AS retweeter,
    retweet.retweetedStatus_retweetCount AS retweet_retweet_count
FROM tweets AS original_tweet, tweets AS retweet
WHERE retweet.retweetedStatus_idStr = original_tweet.idStr
GROUP BY 
    original_tweet_id,
    original_author,
    retweeter,
    retweet_retweet_count
ORDER BY retweet_retweet_count DESC
LIMIT 10;
