SELECT
    orig.orig_id                     AS original_tweet_id,
    orig.orig_user                   AS original_user,
    COUNT(DISTINCT rt.rt_id)         AS total_retweet_rows,
    COUNT(DISTINCT rep.rep_id)       AS total_reply_rows
  FROM
    (
      -- Extract “original” fields from JSON, filtering by lang and source,
      -- and excluding any row that is itself a retweet or a reply
      SELECT
        raw_json->>'id_str'                        AS orig_id,
        raw_json->'user'->>'screen_name'           AS orig_user
      FROM test_table
      WHERE
        raw_json->>'lang'    = 'en'
      
        AND (raw_json->>'in_reply_to_user_id_str') IS NULL
    ) AS orig
    LEFT JOIN
    (
      -- Extract retweet rows: each has its own id_str and the id_str of the original it retweeted
      SELECT
        raw_json->>'id_str'                                AS rt_id,
        raw_json->'retweeted_status'->>'id_str'            AS rt_orig_id
      FROM test_table
     
    ) AS rt
      ON rt.rt_orig_id = orig.orig_id
  
    LEFT JOIN
    (
      -- Extract reply rows: each has its own id_str and the id_str of the original it replied to
      SELECT
        raw_json->>'id_str'                                AS rep_id,
        raw_json->>'in_reply_to_user_id_str'               AS rep_to_id
      FROM test_table
      WHERE
        (raw_json->>'in_reply_to_user_id_str') IS NOT NULL
    ) AS rep
      ON rep.rep_to_id = orig.orig_id
  
  GROUP BY
    orig.orig_id,
    orig.orig_user
  
  ORDER BY
    total_retweet_rows DESC
  LIMIT 25;
