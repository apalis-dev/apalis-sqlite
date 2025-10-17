UPDATE
    Jobs
SET
    status = 'Pending',
    attempt = attempt + 1,
    run_at = ?1,
    done_at = NULL,
    lock_by = NULL
WHERE
    id = ?2
    AND lock_by = ?3
