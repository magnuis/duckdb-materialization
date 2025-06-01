SELECT
    u.raw_json->'user'->>'id_str'       AS user_ider_id,
    u.raw_json->'user'->>'screen_name'  AS screen_name,
    d.raw_json->'delete'->'status'->>'id_str'       AS deleted_status_id,
    d.raw_json->'delete'->>'timestamp_ms'                     AS delete_timestamp
  FROM
    test_table u,
    test_table d
  WHERE
      
        (u.raw_json->>'source') LIKE '%Twitter for iPhone%'
AND
     
     (d.raw_json->'delete'->'status'->>'user_id_str') = (u.raw_json->'user'->>'id_str');
