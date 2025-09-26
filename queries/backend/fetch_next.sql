UPDATE Jobs
SET
    status = 'Running',
    lock_by = ?1,
    lock_at = CURRENT_TIMESTAMP
WHERE
    ROWID IN (
        SELECT ROWID
        FROM Jobs
        WHERE job_type = ?2
            AND status = 'Pending'
            AND lock_by IS NULL
            AND (run_at IS NULL OR run_at <= CURRENT_TIMESTAMP)
        ORDER BY priority DESC, run_at ASC, id ASC
        LIMIT ?3
    )
RETURNING *
