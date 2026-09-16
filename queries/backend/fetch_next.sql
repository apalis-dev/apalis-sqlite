UPDATE
    Jobs
SET
    status = 'Queued',
    lock_by = ?1,
    lock_at = strftime('%s', 'now')
WHERE
    ROWID IN (
        WITH candidates AS (
            SELECT
                *
            FROM
                (
                    SELECT
                        ROWID,
                        priority,
                        run_at,
                        id
                    FROM
                        Jobs
                    WHERE
                        job_type = ?2
                        AND status = 'Pending'
                        AND lock_by IS NULL
                        AND (
                            run_at IS NULL
                            OR run_at <= ?4
                        )
                    ORDER BY
                        priority DESC,
                        run_at ASC,
                        id ASC
                    LIMIT
                        ?3
                )
            UNION
            ALL
            SELECT
                *
            FROM
                (
                    SELECT
                        ROWID,
                        priority,
                        run_at,
                        id
                    FROM
                        Jobs
                    WHERE
                        job_type = ?2
                        AND status = 'Failed'
                        AND attempts < max_attempts
                        AND (
                            run_at IS NULL
                            OR run_at <= ?4
                        )
                    ORDER BY
                        priority DESC,
                        run_at ASC,
                        id ASC
                    LIMIT
                        ?3
                )
        )
        SELECT
            ROWID
        FROM
            candidates
        ORDER BY
            priority DESC,
            run_at ASC,
            id ASC
        LIMIT
            ?3
    ) RETURNING *
