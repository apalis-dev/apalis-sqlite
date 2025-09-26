UPDATE Jobs
SET
    status = 'Running',
    lock_at = CURRENT_TIMESTAMP
WHERE ROWID IN (
    SELECT ROWID
    FROM Jobs
    WHERE job_type IN (
            SELECT value FROM json_each(?1)
        )
        AND status = 'Pending'
        AND lock_by IS NULL
        AND (
            run_at IS NULL
            OR run_at <= CURRENT_TIMESTAMP
        )
        AND ROWID IN (
            SELECT value FROM json_each(?2)
        )
    ORDER BY
        priority DESC,
        run_at ASC,
        id ASC
    LIMIT ?3
)
RETURNING *;
