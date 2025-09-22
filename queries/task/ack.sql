UPDATE
    Jobs
SET
    status = ?4,
    attempts = ?2,
    last_error = ?3,
    done_at = CURRENT_TIMESTAMP
WHERE
    id = ?1
    AND lock_by = ?5
