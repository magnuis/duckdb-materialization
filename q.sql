WITH extracted AS (
    SELECT json_extract_string(raw_json, ['n_name','n_nationkey','c_nationkey', 'c_name']) as extracted_list
    FROM test_table
)
SELECT
    c.extracted_list[4]
FROM 
    extracted c,
    extracted n
WHERE 
    n.extracted_list[2]::INT = c.extracted_list[3]::INT
    AND n.extracted_list[1] = 'MOZAMBIQUE'
LIMIT 10;


SELECT
    c.raw_json->>'c_name'
FROM 
    test_table c,
    test_table n
WHERE 
    n.raw_json->>'n_name' = 'MOZAMBIQUE'
    AND CAST(n.raw_json->>'n_nationkey' AS INT) = CAST(c.raw_json->>'c_nationkey' AS INT)
LIMIT 10;


ALTER TABLE test_table ADD COLUMN n_nationkey INT;
ALTER TABLE test_table ADD COLUMN c_nationkey INT;
UPDATE test_table SET n_nationkey = raw_json->>'n_nationkey';
UPDATE test_table SET c_nationkey = raw_json->>'c_nationkey';

WITH extracted AS (
    SELECT
        n_nationkey,
        c_nationkey,
        json_extract_string(raw_json, ['n_name', 'c_name']) as extracted_list
    FROM test_table
)
SELECT
    c.extracted_list[2]
FROM 
    extracted c,
    extracted n
WHERE 
    n.n_nationkey = c.c_nationkey
    AND n.extracted_list[1] = 'MOZAMBIQUE'
LIMIT 10;


SELECT
    c.raw_json->>'c_name'
FROM 
    test_table c,
    test_table n
WHERE 
    n.raw_json->>'n_name' = 'MOZAMBIQUE'
    AND n.n_nationkey = c.c_nationkey
LIMIT 10;

ALTER TABLE test_table ADD COLUMN n_name VARCHAR;
UPDATE test_table SET n_name = raw_json->>'n_name';

WITH extracted AS (
    SELECT
        n_nationkey,
        c_nationkey,
        n_name, 
        json_extract_string(raw_json, ['c_name']) as extracted_list
    FROM test_table
)
SELECT
    c.extracted_list[1]
FROM 
    extracted c,
    extracted n
WHERE 
    n.n_nationkey = c.c_nationkey
    AND n.n_name = 'MOZAMBIQUE'
LIMIT 10;


SELECT
    c.raw_json->>'c_name'
FROM 
    test_table c,
    test_table n
WHERE 
    n.n_name = 'MOZAMBIQUE'
    AND n.n_nationkey = c.c_nationkey
LIMIT 10;