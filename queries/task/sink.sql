INSERT INTO
    Jobs
VALUES
    (
        ?1,
        ?2,
        ?3,
        'Pending',
        0,
        ?4,
        ?5,
        NULL,
        NULL,
        NULL,
        NULL,
        ?6,
        ?7,
        ?8
    ) ON CONFLICT(job_type, idempotency_key) DO NOTHING
