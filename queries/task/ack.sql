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
    -- A deferred task goes back to Pending and must be unlocked to be fetched again.
    lock_by = CASE WHEN j.status = 'Pending' THEN NULL ELSE lock_by END,
    lock_at = CASE WHEN j.status = 'Pending' THEN NULL ELSE lock_at END,
    done_at = CASE WHEN j.status = 'Pending' THEN NULL ELSE strftime('%s', 'now') END
FROM
    j
WHERE
    Jobs.id = j.task_id
    AND Jobs.lock_by = ?2
