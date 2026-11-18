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
    AND done_at <= strftime('%s', 'now') - ?1;

VACUUM;
