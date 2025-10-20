SELECT
    id,
    status,
    last_result as result
FROM
    Jobs
WHERE
    id IN (
        SELECT
            value
        FROM
            json_each(?)
    )
    AND (
        status = 'Done'
        OR (
            status = 'Failed'
            AND attempts >= max_attempts
        )
        OR status = 'Killed'
    )
