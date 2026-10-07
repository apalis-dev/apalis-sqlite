DELETE from
    Jobs
WHERE
    (
        status = 'Done'
        OR status = 'Killed'
        OR (
            status = 'Failed'
            AND max_attempts <= attempts
        )
    )
    AND (
        run_at < datetime('now', '-' || ?1 || ' seconds')
    );

VACUUM;
