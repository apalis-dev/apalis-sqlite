Select
    COUNT(*) AS count
FROM
    Jobs
WHERE
    (
        status = 'Pending'
        OR (
            status = 'Failed'
            AND attempts < max_attempts
        )
    )
