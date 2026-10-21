WITH stats AS (
    SELECT
        job_type,
        COUNT(*) FILTER (WHERE status = 'Running') AS running_jobs,
        COUNT(*) FILTER (WHERE status = 'Pending') AS pending_jobs,
        COUNT(*) FILTER (WHERE status = 'Failed') AS failed_jobs,
        COUNT(*) FILTER (WHERE status = 'Done') AS done_jobs,
        COUNT(*) FILTER (WHERE status = 'Killed') AS killed_jobs,
        COUNT(*) FILTER (WHERE status IN ('Pending', 'Queued', 'Running')) AS active_jobs,
        SUM(CASE WHEN status = 'Running' AND run_at < strftime('%s', 'now', '-1 hour') THEN 1 END) AS stale_running_jobs,
        SUM(CASE WHEN run_at >= strftime('%s', 'now', '-1 hour') THEN 1 END) AS jobs_past_hour,
        SUM(CASE WHEN date(run_at, 'unixepoch') = date('now') THEN 1 END) AS jobs_today,
        SUM(CASE WHEN date(run_at, 'unixepoch') = date('now') AND status = 'Killed' THEN 1 END) AS killed_jobs_today,
        SUM(CASE WHEN run_at >= strftime('%s', 'now', '-7 days') THEN 1 END) AS jobs_past_7_days,
        ROUND(
            AVG((done_at - run_at) / 60.0) FILTER (WHERE status IN ('Done', 'Failed', 'Killed') AND done_at IS NOT NULL),
            2
        ) AS avg_job_duration_mins,
        MAX(CASE WHEN status = 'Running' THEN (strftime('%s', 'now') - run_at) / 60.0 ELSE 0 END) AS longest_running_job_mins,
        COUNT(*) AS total_jobs,
        MAX(run_at) AS most_recent_job
    FROM
        Jobs
    GROUP BY
        job_type
),
activity AS (
    SELECT
        job_type,
        json_group_array(daily_count) AS activity
    FROM (
        SELECT
            job_type,
            date(run_at, 'unixepoch') AS run_date,
            COUNT(*) AS daily_count
        FROM
            Jobs
        WHERE
            run_at >= strftime('%s', 'now', '-7 days')
        GROUP BY
            job_type, date(run_at, 'unixepoch')
        ORDER BY
            job_type, run_date
    ) daily
    GROUP BY
        job_type
),
lockers AS (
    SELECT
        job_type,
        json_group_array(lock_by) AS workers
    FROM (
        SELECT
            DISTINCT job_type, lock_by
        FROM
            Jobs
        WHERE
            lock_by IS NOT NULL
        ORDER BY
            job_type, lock_by
    ) distinct_lockers
    GROUP BY
        job_type
),
queue_stats AS (
    SELECT
        job_type,
        json_group_array(
            json_object(
                'title', statistic,
                'stat_type', type,
                'value', value,
                'priority', priority
            )
        ) AS stats
    FROM (
        SELECT
            *
        FROM (
            SELECT
                job_type, priority, type, statistic, value
            FROM (
                SELECT s.job_type, 1 AS priority, 'Number' AS type, 'RUNNING_JOBS' AS statistic, CAST(s.running_jobs AS TEXT) AS value
                FROM stats s
                UNION ALL
                SELECT s.job_type, 1, 'Number', 'PENDING_JOBS', CAST(s.pending_jobs AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 1, 'Number', 'FAILED_JOBS', CAST(s.failed_jobs AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 2, 'Number', 'ACTIVE_JOBS', CAST(s.active_jobs AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 2, 'Number', 'STALE_RUNNING_JOBS', CAST(s.stale_running_jobs AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 2, 'Percentage', 'KILL_RATE', CAST(ROUND(100.0 * s.killed_jobs / NULLIF(s.total_jobs, 0), 2) AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 3, 'Number', 'JOBS_PAST_HOUR', CAST(s.jobs_past_hour AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 3, 'Number', 'JOBS_TODAY', CAST(s.jobs_today AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 3, 'Number', 'KILLED_JOBS_TODAY', CAST(CASE WHEN s.jobs_today IS NOT NULL THEN COALESCE(s.killed_jobs_today, 0) END AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 3, 'Decimal', 'AVG_JOBS_PER_MINUTE_PAST_HOUR', CAST(CASE WHEN s.jobs_past_hour IS NOT NULL THEN ROUND(s.jobs_past_hour / 60.0, 2) END AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 4, 'Number', 'TOTAL_JOBS', CAST(s.total_jobs AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 4, 'Number', 'DONE_JOBS', CAST(s.done_jobs AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 4, 'Number', 'KILLED_JOBS', CAST(s.killed_jobs AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 4, 'Percentage', 'SUCCESS_RATE', CAST(ROUND(100.0 * s.done_jobs / NULLIF(s.total_jobs, 0), 2) AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 5, 'Decimal', 'AVG_JOB_DURATION_MINS', CAST(s.avg_job_duration_mins AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 5, 'Decimal', 'LONGEST_RUNNING_JOB_MINS', CAST(ROUND(s.longest_running_job_mins, 2) AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 6, 'Number', 'JOBS_PAST_7_DAYS', CAST(s.jobs_past_7_days AS TEXT)
                FROM stats s
                UNION ALL
                SELECT s.job_type, 8, 'Timestamp', 'MOST_RECENT_JOB', CAST(s.most_recent_job AS TEXT)
                FROM stats s
            ) raw
            WHERE
                value IS NOT NULL
            ORDER BY
                job_type, priority, statistic
        ) ordered
    ) grouped
    GROUP BY
        job_type
),
all_job_types AS (
    SELECT
        worker_type AS job_type
    FROM
        Workers
    UNION
    SELECT
        job_type
    FROM
        stats
)
SELECT
    jt.job_type as name,
    COALESCE(qs.stats, '[]') as stats,
    COALESCE(lk.workers, '[]') as workers,
    COALESCE(a.activity, '[]') as activity
FROM
    all_job_types jt
    LEFT JOIN queue_stats qs ON jt.job_type = qs.job_type
    LEFT JOIN lockers lk ON jt.job_type = lk.job_type
    LEFT JOIN activity a ON jt.job_type = a.job_type
ORDER BY
    name;
