SELECT
    CAST(COUNT(*) AS INTEGER) AS total,
        CAST(SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) AS INTEGER) AS pending,
        CAST(SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END) AS INTEGER) AS running,
        CAST(SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) AS INTEGER) AS done,
        CAST(SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS INTEGER) AS failed,
        CAST(SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) AS INTEGER) AS killed,
        CAST(SUM(CASE WHEN status IN ('Done', 'Failed', 'Killed') THEN 1 ELSE 0 END) AS INTEGER) AS completed,
        CAST(SUM(CASE WHEN status IN ('Pending', 'Running') THEN 1 ELSE 0 END) AS INTEGER) AS active
FROM Jobs 
WHERE job_type = ?1
