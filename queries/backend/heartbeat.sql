INSERT INTO
    Workers (id, worker_type, storage_name, layers, last_seen)
VALUES
    ($1, $2, $3, $4, $5) ON CONFLICT (id) DO
UPDATE
SET
    last_seen = EXCLUDED.last_seen
