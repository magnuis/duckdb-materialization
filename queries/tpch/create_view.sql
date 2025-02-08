CREATE VIEW test_view AS
SELECT 
    CAST(raw_json->>'r_regionkey' AS INT) AS r_regionkey,
    raw_json->>'r_name' AS r_name,
    raw_json->>'r_comment' AS r_comment
FROM test_table;