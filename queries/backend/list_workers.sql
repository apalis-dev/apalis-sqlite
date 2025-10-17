SELECT
    *
FROM
    Workers
WHERE
    worker_type = ?1
ORDER BY
    last_seen DESC
LIMIT
    ?2 OFFSET ?3
