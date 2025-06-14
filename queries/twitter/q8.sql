SELECT
  ANY_VALUE((user_info.raw_json->'user'->>'screen_name'))                        AS screen_name,
  SUM(TRY_CAST((user_info.raw_json->'user'->>'followers_count') AS INT))           AS followers_count,
  COUNT(DISTINCT (reply.raw_json->>'id_str'))                                      AS reply_count,
  COUNT(DISTINCT (retweet.raw_json->>'id_str'))                                    AS retweet_count
FROM
  test_table user_info,
  test_table retweet,
 test_table reply
WHERE
  TRY_CAST((user_info.raw_json->'user'->>'followers_count') AS INT) > 1000
  AND (reply.raw_json->>'in_reply_to_user_id_str') = (user_info.raw_json->'user'->>'id_str')
  AND (retweet.raw_json->'retweeted_status'->'user'->>'id_str')   = (user_info.raw_json->'user'->>'id_str')
GROUP BY
  (user_info.raw_json->'user'->>'id_str')
ORDER BY
  followers_count DESC,
  (reply_count + retweet_count) DESC
LIMIT 15;
