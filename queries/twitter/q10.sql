SELECT
  orig.raw_json->>'source'                                AS source,
  COUNT(DISTINCT orig.raw_json->>'id_str')                 AS num_original_tweets,
  COUNT(DISTINCT rt.raw_json->>'id_str')                   AS num_retweet_rows,
  COUNT(DISTINCT rep.raw_json->>'id_str' )                  AS num_reply_rows,
  COALESCE(SUM(TRY_CAST(rt.raw_json->'retweeted_status'->>'retweet_count' AS INT)), 0)           AS total_retweetCount
FROM

   test_table orig
  LEFT JOIN  test_table rt
    ON (rt.raw_json->'retweeted_status'->>'id_str') = (orig.raw_json->>'id_str') 
  LEFT JOIN test_table rep 
    ON (rep.raw_json->>'in_reply_to_user_id_str') = (orig.raw_json->>'in_reply_to_user_id_str')
WHERE
  (orig.raw_json->'retweeted_status'->>'id_str')  IS NULL
  AND (orig.raw_json->>'in_reply_to_user_id_str') IS NULL
GROUP BY
  (orig.raw_json->>'source'  )
ORDER BY
  num_original_tweets DESC;
