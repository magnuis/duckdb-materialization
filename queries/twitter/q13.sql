SELECT
  t1.idStr AS original_tweet_id,
  t1.user_screenName AS original_user,
  t2.idStr AS retweet_id,
  t2.user_screenName AS retweeter
FROM
  test_table AS t1
  JOIN test_table AS t2
    ON t2.retweetedStatus_idStr = t1.idStr
WHERE
  -- (1) SARGable predicate: can be applied during table scan/index seek
  t1.user_followersCount > 1000

  -- (2) Non‐SARGable predicate: can’t be applied during an index scan (uses a function on a column)
  AND LOWER(t1.text) LIKE '%covid-19%'

  -- (3) Another SARGable predicate (on the joined table)
  AND t2.source = 'Twitter for iPhone';

