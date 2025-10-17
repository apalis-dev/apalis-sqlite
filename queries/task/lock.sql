UPDATE
    Jobs
SET
    status = 'Running',
    lock_at = strftime('%s', 'now'),
    lock_by = ?2
WHERE
    id = ?1
    AND (
        status = 'Queued'
        OR status = 'Pending'
        OR (
            status = 'Failed'
            AND attempts < max_attempts
        )
    )
