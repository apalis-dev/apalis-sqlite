WITH j AS (
    SELECT
        value ->> 'task_id' AS task_id,
        value ->> 'attempt' AS attempt,
        value ->> 'result' AS result,
        value ->> 'status' AS status
    FROM
        json_each(?1)
)
UPDATE
    Jobs
SET
    status = j.status,
    attempts = j.attempt,
    last_result = j.result,
    done_at = strftime('%s', 'now')
FROM
    j
WHERE
    Jobs.id = j.task_id
    AND Jobs.lock_by = ?2
