SELECT
  t_orig.idStr                    AS original_tweet_id,
  t_orig.user_screenName          AS original_user,
  t_orig.user_followersCount      AS original_user_followers,
  COUNT(DISTINCT t_rt.user_screenName)      AS num_distinct_retweeters,
  COALESCE(SUM(t_rt.user_followersCount), 0) AS total_followers_of_retweeters,
  COALESCE(MAX(t_rt.retweetedStatus_retweetCount), 0) AS max_retweetCount_among_retweets,
  COUNT(DISTINCT t_rep.idStr)        AS num_distinct_replies,
  ROUND(
    100.0 * COUNT(DISTINCT t_rt.user_screenName)
          / NULLIF((SELECT COUNT(DISTINCT user_screenName) FROM test_table), 0),
    2
  ) AS pct_of_all_users_who_retweeted
FROM
  test_table AS t_orig
  /* Only keep “original” tweets (not retweets, not replies) */
  LEFT JOIN test_table AS t_rt
    ON t_rt.retweetedStatus_idStr = t_orig.idStr
  LEFT JOIN test_table AS t_rep
    ON t_rep.inReplyToUserIdStr = t_orig.user_screenName
WHERE
  t_orig.retweetedStatus_idStr IS NULL
  AND t_orig.inReplyToUserIdStr      IS NULL
GROUP BY
  t_orig.idStr,
  t_orig.user_screenName,
  t_orig.user_followersCount
ORDER BY
  num_distinct_retweeters DESC
LIMIT 50;
