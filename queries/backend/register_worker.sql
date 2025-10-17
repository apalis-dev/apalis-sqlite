INSERT INTO
    Workers (id, worker_type, storage_name, layers, last_seen)
SELECT
    ?1,
    ?2,
    ?3,
    ?4,
    strftime('%s', 'now')
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            Workers
        WHERE
            id = ?1
            AND strftime('%s', 'now') - last_seen >= ?5
    );
