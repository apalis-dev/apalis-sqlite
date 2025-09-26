SELECT
    *
FROM
    Jobs
WHERE
    status = ?1
    AND job_type = ?2
ORDER BY
    done_at DESC,
    run_at DESC
LIMIT
    ?3 OFFSET ?4
