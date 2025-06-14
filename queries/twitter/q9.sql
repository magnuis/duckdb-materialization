SELECT
  (orig.raw_json->>'id_str')                                    AS original_tweet_id,
  (orig.raw_json->'user'->>'user_screen_name')                           AS original_user,
  (orig.raw_json->'user'->>'followers_count')                            AS original_user_followers,
  COUNT(DISTINCT rt.raw_json->'user'->>'screen_name' )           AS num_distinct_retweeters,
  COALESCE(SUM(TRY_CAST(rt.raw_json->'user'->>'followers_count' AS INT)), 0)          AS total_followers_of_retweeters,
  COALESCE(MAX(TRY_CAST(rt.raw_json->'retweeted_status'->>'retweet_count' AS INT)), 0)            AS max_retweetCount_among_retweets,
  COUNT(DISTINCT rep.raw_json->>'id_str')                   AS num_distinct_replies,
  ROUND(
    100.0
    * COUNT(DISTINCT rt.raw_json->'user'->>'screen_name' )
    / NULLIF(
        (
          SELECT COUNT(DISTINCT (sub.raw_json->'user'->>'user_screen_name'))
            FROM test_table AS sub
        ),
        0
      ),
    2
  )                                                AS pct_of_all_users_who_retweeted
FROM
  test_table orig
  LEFT JOIN  test_table  rt
    ON (rt.raw_json->'retweeted_status'->>'id_str')  = (orig.raw_json->>'id_str')
  LEFT JOIN 
     test_table rep
    ON (rep.raw_json->>'in_reply_to_user_id_str')  = (orig.raw_json->'user'->>'user_screen_name')
WHERE
  (orig.raw_json->'retweeted_status'->>'id_str') IS NULL
  AND (orig.raw_json->>'in_reply_to_user_id_str' ) IS NULL
GROUP BY
  (orig.raw_json->>'id_str'),
  (orig.raw_json->'user'->>'user_screen_name'),
  (orig.raw_json->'user'->>'followers_count')
ORDER BY
  num_distinct_retweeters DESC
LIMIT 50;
