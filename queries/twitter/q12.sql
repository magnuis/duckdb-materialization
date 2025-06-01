SELECT
  (orig.raw_json->>'id_str')                                               AS original_tweet_id,
  (orig.raw_json->'user'->>'screen_name')                                   AS original_user,
  COUNT(DISTINCT (rt.raw_json->>'id_str'))                                   AS total_retweet_rows,
  COUNT(DISTINCT (rep.raw_json->>'id_str'))                                  AS total_reply_rows
FROM
  test_table AS orig
  LEFT JOIN test_table AS rt
    ON (rt.raw_json->'retweeted_status'->>'id_str') 
       = (orig.raw_json->>'id_str')
  LEFT JOIN test_table AS rep
    ON (rep.raw_json->>'in_reply_to_user_id_str') 
       = (orig.raw_json->>'id_str')
WHERE
  TRY_CAST(orig.raw_json->'user'->>'is_translator' AS BOOLEAN)
  AND ((orig.raw_json->>'in_reply_to_user_id_str') IS NULL)
GROUP BY
  (orig.raw_json->>'id_str'),
  (orig.raw_json->'user'->>'screen_name')
ORDER BY
  total_retweet_rows DESC
LIMIT 25;
