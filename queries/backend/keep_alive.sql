UPDATE
    Workers
SET
    last_seen = strftime('%s', 'now')
WHERE
    id = $1 AND worker_type = $2;
