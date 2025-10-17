UPDATE
    Jobs
SET
    status = ?1,
    attempts = ?2,
    done_at = ?3,
    lock_by = ?4,
    lock_at = ?5,
    last_result = ?6,
    priority = ?7
WHERE
    id = ?8
