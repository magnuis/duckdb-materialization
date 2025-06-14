SELECT
  (orig.raw_json->>'id_str')                                               AS original_tweet_id,
  COUNT(DISTINCT (rt.raw_json->>'id_str'))                                   AS num_retweets,
  COUNT(DISTINCT (rep.raw_json->>'id_str'))                                  AS num_replies
FROM
  test_table AS orig
  LEFT JOIN test_table AS rt
    ON (rt.raw_json->'retweeted_status'->>'id_str') 
       = (orig.raw_json->>'id_str')
    AND LOWER(rt.raw_json->>'text') LIKE '%bad%'
  LEFT JOIN test_table AS rep
    ON (rep.raw_json->>'in_reply_to_user_id_str') 
       = (orig.raw_json->>'id_str')
WHERE
  (orig.raw_json->>'lang') = 'en'
  AND TRY_CAST((orig.raw_json->'user'->>'followers_count') AS INT) > 75
GROUP BY
  (orig.raw_json->>'id_str')
ORDER BY
  num_retweets DESC,
  num_replies DESC
LIMIT 25;
