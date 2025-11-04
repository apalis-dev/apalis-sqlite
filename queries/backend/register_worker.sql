INSERT INTO
    Workers (id, worker_type, storage_name, layers, last_seen, started_at)
VALUES (
    ?1,
    ?2,
    ?3,
    ?4,
    strftime('%s', 'now'),
    strftime('%s', 'now')
)
ON CONFLICT(id) DO UPDATE SET
    worker_type = excluded.worker_type,
    storage_name = excluded.storage_name,
    layers = excluded.layers,
    last_seen = excluded.last_seen,
    started_at = excluded.started_at,
    -- Force a constraint violation to throw an error
    id = CASE
        WHEN strftime('%s', 'now') - Workers.last_seen <= ?5
        THEN NULL  -- This will fail if id has NOT NULL constraint
        ELSE excluded.id
    END
WHERE
    strftime('%s', 'now') - Workers.last_seen > ?5;
