UPDATE Jobs
SET
    status = 'Queued',
    lock_by = ?1,
    lock_at = strftime('%s', 'now')
WHERE
    ROWID IN (
        SELECT ROWID
        FROM Jobs
        WHERE job_type = ?2
            AND (
                    (status = 'Pending' AND lock_by IS NULL) 
                    OR 
                    (status = 'Failed' AND attempts < max_attempts)
                )
            AND (run_at IS NULL OR run_at <= strftime('%s', 'now'))
        ORDER BY priority DESC, run_at ASC, id ASC
        LIMIT ?3
    )
RETURNING *
