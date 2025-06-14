SELECT ROUND(
    100.0 * COUNT(DISTINCT user_idStr) / 
    (SELECT COUNT(DISTINCT user_idStr) FROM test_table), 2) AS percentage
FROM test_table
WHERE lower(text) LIKE '%covid-19%';
