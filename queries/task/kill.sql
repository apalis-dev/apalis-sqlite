UPDATE
    Jobs
SET
    status = 'Killed',
    done_at = strftime('%s', 'now'),
    last_result = ?3
WHERE
    id = ?1
    AND lock_by = ?2
