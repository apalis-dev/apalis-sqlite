SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) AS pending,
    SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END) AS running,
    SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) AS done,
    SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) AS killed,
    SUM(CASE WHEN status IN ('Done', 'Failed', 'Killed') THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN status IN ('Pending', 'Running') THEN 1 ELSE 0 END) AS active
FROM Jobs 
WHERE job_type = ?1
