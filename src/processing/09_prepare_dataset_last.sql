CREATE TABLE dataset_new_6m_last AS
SELECT *, 'terminated' AS collaboration FROM terminated_6m_last
UNION ALL
SELECT *, 'triggered' AS collaboration FROM triggered_6m_last
UNION ALL
SELECT *, 'sustained' AS collaboration FROM sustained_6m_last
UNION ALL
SELECT *, 'temporary' AS collaboration FROM temporary_6m_last;

SELECT 
    collaboration,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM dataset_new_6m_last
GROUP BY collaboration
ORDER BY collaboration;

CREATE TABLE dataset_new_2y_last AS
SELECT *, 'terminated' AS collaboration FROM terminated_2y_last
UNION ALL
SELECT *, 'triggered' AS collaboration FROM triggered_2y_last
UNION ALL
SELECT *, 'sustained' AS collaboration FROM sustained_2y_last
UNION ALL
SELECT *, 'temporary' AS collaboration FROM temporary_2y_last;

SELECT 
    collaboration,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM dataset_new_2y_last
GROUP BY collaboration
ORDER BY collaboration;