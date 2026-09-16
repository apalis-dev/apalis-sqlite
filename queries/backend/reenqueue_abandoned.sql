UPDATE
    Jobs
SET
    status = "Pending",
    done_at = NULL,
    lock_by = NULL,
    lock_at = NULL,
    attempts = attempts + 1,
    last_result = '{"Err": "Re-enqueued due to worker shutdown"}'
WHERE
    id IN (
        SELECT
            Jobs.id
        FROM
            Jobs
            INNER JOIN Workers ON lock_by = Workers.id
        WHERE
            status = "Queued"
            AND Workers.worker_type = ?1
            AND Workers.id = ?2
            AND Jobs.id IN (
                SELECT
                    value
                FROM
                    json_each(?3)
            )
    );
