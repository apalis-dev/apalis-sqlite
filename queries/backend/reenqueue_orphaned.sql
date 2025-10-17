UPDATE
    Jobs
SET
    status = "Pending",
    done_at = NULL,
    lock_by = NULL,
    lock_at = NULL,
    attempts = attempts + 1,
    last_error = '{"Err": "Re-enqueued due to worker heartbeat timeout."}'
WHERE
    id IN (
        SELECT
            Jobs.id
        FROM
            Jobs
            INNER JOIN Workers ON lock_by = Workers.id
        WHERE
            (
                status = "Running"
                OR status = "Queued"
            )
            AND strftime('%s', 'now') - Workers.last_seen >= ?1
            AND Workers.worker_type = ?2
    );
