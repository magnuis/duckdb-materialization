SELECT
  t_orig.idStr                              AS original_tweet_id,
  t_orig.user_screenName                    AS original_user,
  
  -- 1) How many unique users retweeted this original?
  COUNT(DISTINCT t_rt.user_screenName)       AS num_distinct_retweeters,
  
  -- 2) How many unique tweets replied directly to the original author?
  COUNT(DISTINCT t_rep.idStr)                AS num_distinct_replies_to_original,
  
  -- 3) How many unique tweets replied to any of those retweeters?
  COUNT(DISTINCT t_rt_rep.idStr)             AS num_distinct_replies_to_retweeters,
  
  -- 4) Sum of the “retweetCount” field carried by each retweet
  COALESCE(
    SUM(t_rt.retweetedStatus_retweetCount),
    0
  )                                          AS total_retweetCount
FROM
  test_table AS t_orig

  /* 1) Join in all retweet‐rows of this original tweet */
  LEFT JOIN test_table AS t_rt
    ON t_rt.retweetedStatus_idStr = t_orig.idStr

  /* 2) Join in all replies to the original’s user */
  LEFT JOIN test_table AS t_rep
    ON t_rep.inReplyToUserIdStr = t_orig.user_screenName

  /* 3) Join in all replies that target any of the retweeters (i.e. reply‐to‐retweeter) */
  LEFT JOIN test_table AS t_rt_rep
    ON t_rt_rep.inReplyToUserIdStr = t_rt.user_screenName

WHERE
  /* Only consider “original” tweets: no retweetedStatus_idStr, no inReplyToUserIdStr */
  t_orig.retweetedStatus_idStr IS NULL
  AND t_orig.inReplyToUserIdStr      IS NULL

GROUP BY
  t_orig.idStr,
  t_orig.user_screenName

ORDER BY
  num_distinct_retweeters DESC

LIMIT 50;
