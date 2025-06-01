SELECT
  t_orig.source                                    AS source,
  COUNT(DISTINCT t_orig.idStr)                     AS num_original_tweets,
  COUNT(DISTINCT t_rt.idStr)                       AS num_retweet_rows,
  COUNT(DISTINCT t_rep.idStr)                      AS num_reply_rows,
  COALESCE(SUM(t_rt.retweetedStatus_retweetCount), 0) AS total_retweetCount
FROM
  -- 1) “Original” tweets: those that are neither retweets nor replies
  test_table AS t_orig
  LEFT JOIN test_table AS t_rt
    ON t_rt.retweetedStatus_idStr = t_orig.idStr
      -- every row in t_rt that has retweetedStatus_idStr = t_orig.idStr is a retweet of that original
  LEFT JOIN test_table AS t_rep
    ON t_rep.inReplyToUserIdStr = t_orig.user_screenName
      -- every row in t_rep whose inReplyToUserIdStr = t_orig.user_screenName is counted as a “reply” 
      -- to the original user (since we only have inReplyToUserIdStr, not a reply‐to‐statusID)
WHERE
  t_orig.retweetedStatus_idStr IS NULL
  AND t_orig.inReplyToUserIdStr      IS NULL
  -- ensures t_orig rows are standalone “original” tweets
GROUP BY
  t_orig.source
ORDER BY
  num_original_tweets DESC;
