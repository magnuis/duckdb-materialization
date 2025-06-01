SELECT
    (d1.raw_json->'delete'->'status'->>'user_id_str')                                  AS deleted_user_id,
    (MAX(TRY_CAST((d1.raw_json->'delete'->>'timestamp_ms') AS BIGINT))
     - MIN(TRY_CAST((d1.raw_json->'delete'->>'timestamp_ms') AS BIGINT)))              AS delete_time_diff_ms
  FROM
    test_table AS d1,
    test_table AS d2
  WHERE
    (d1.raw_json->'delete')             IS NOT NULL
    AND (d2.raw_json->'delete')         IS NOT NULL
    -- Join on the same deleted_user_id to ensure at least two deletions exist
    AND (d1.raw_json->'delete'->'status'->>'user_id_str')
        = (d2.raw_json->'delete'->'status'->>'user_id_str')
  GROUP BY
    (d1.raw_json->'delete'->'status'->>'user_id_str')
  HAVING
     delete_time_diff_ms > 0
  ORDER BY
    delete_time_diff_ms DESC;
