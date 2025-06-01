SELECT
  t_orig.idStr                     AS original_tweet_id,
  t_orig.user_screenName           AS original_user,
  COUNT(DISTINCT t_rt.idStr)       AS total_retweet_rows,
  COUNT(DISTINCT t_rep.idStr)      AS total_reply_rows
FROM
  test_table AS t_orig

  /* 1st JOIN: all retweets of the original */
  LEFT JOIN test_table AS t_rt
    ON t_rt.retweetedStatus_idStr = t_orig.idStr

  /* 2nd JOIN: all replies to the original */
  LEFT JOIN test_table AS t_rep
    ON t_rep.inReplyToStatusIdStr = t_orig.idStr

WHERE
  /* Only keep originals that are English AND from “Twitter for iPhone” */
  t_orig.lang   = 'en'
  AND t_orig.source = 'Twitter for iPhone'

  /* Exclude any rows where t_orig itself is a retweet or reply */
  AND t_orig.retweetedStatus_idStr IS NULL
  AND t_orig.inReplyToUserIdStr      IS NULL

GROUP BY
  t_orig.idStr,
  t_orig.user_screenName

ORDER BY
  total_retweet_rows DESC
LIMIT 25;
