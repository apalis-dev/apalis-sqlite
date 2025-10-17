UPDATE
    Jobs
SET
    status = 'Failed',
    attempts = attempts + 1,
    done_at = NULL,
    lock_by = NULL,
    lock_at = NULL,
    run_at = ?2
WHERE
    id = ?1
