CREATE TABLE dataset_new_6m AS
SELECT *, 'terminated' AS collaboration FROM terminated_6m
UNION ALL
SELECT *, 'triggered' AS collaboration FROM triggered_6m
UNION ALL
SELECT *, 'sustained' AS collaboration FROM sustained_6m
UNION ALL
SELECT *, 'temporary' AS collaboration FROM temporary_6m;

SELECT 
    collaboration,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM dataset_new_6m
GROUP BY collaboration
ORDER BY collaboration;


CREATE TABLE dataset_new_2y AS
SELECT *, 'terminated' AS collaboration FROM terminated_2y
UNION ALL
SELECT *, 'triggered' AS collaboration FROM triggered_2y
UNION ALL
SELECT *, 'sustained' AS collaboration FROM sustained_2y
UNION ALL
SELECT *, 'temporary' AS collaboration FROM temporary_2y;

SELECT 
    collaboration,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM dataset_new_2y
GROUP BY collaboration
ORDER BY collaboration;