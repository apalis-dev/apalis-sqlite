WITH queue_stats AS (
    SELECT
        job_type,
        json_group_array(
            json_object(
                'title',
                statistic,
                'stat_type',
                type,
                'value',
                value,
                'priority',
                priority
            )
        ) as stats
    FROM
        (
            SELECT
                job_type,
                1 AS priority,
                'Number' AS type,
                'RUNNING_JOBS' AS statistic,
                CAST(
                    SUM(
                        CASE
                            WHEN status = 'Running' THEN 1
                            ELSE 0
                        END
                    ) AS TEXT
                ) AS value
            FROM
                Jobs
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                1,
                'Number',
                'PENDING_JOBS',
                CAST(
                    SUM(
                        CASE
                            WHEN status = 'Pending' THEN 1
                            ELSE 0
                        END
                    ) AS TEXT
                )
            FROM
                Jobs
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                1,
                'Number',
                'FAILED_JOBS',
                CAST(
                    SUM(
                        CASE
                            WHEN status = 'Failed' THEN 1
                            ELSE 0
                        END
                    ) AS TEXT
                )
            FROM
                Jobs
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                2,
                'Number',
                'ACTIVE_JOBS',
                CAST(
                    SUM(
                        CASE
                            WHEN status IN ('Pending', 'Queued', 'Running') THEN 1
                            ELSE 0
                        END
                    ) AS TEXT
                )
            FROM
                Jobs
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                2,
                'Number',
                'STALE_RUNNING_JOBS',
                CAST(COUNT(*) AS TEXT)
            FROM
                Jobs
            WHERE
                status = 'Running'
                AND run_at < strftime('%s', 'now', '-1 hour')
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                2,
                'Percentage',
                'KILL_RATE',
                CAST(
                    ROUND(
                        100.0 * SUM(
                            CASE
                                WHEN status = 'Killed' THEN 1
                                ELSE 0
                            END
                        ) / NULLIF(COUNT(*), 0),
                        2
                    ) AS TEXT
                )
            FROM
                Jobs
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                3,
                'Number',
                'JOBS_PAST_HOUR',
                CAST(COUNT(*) AS TEXT)
            FROM
                Jobs
            WHERE
                run_at >= strftime('%s', 'now', '-1 hour')
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                3,
                'Number',
                'JOBS_TODAY',
                CAST(COUNT(*) AS TEXT)
            FROM
                Jobs
            WHERE
                date(run_at, 'unixepoch') = date('now')
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                3,
                'Number',
                'KILLED_JOBS_TODAY',
                CAST(
                    SUM(
                        CASE
                            WHEN status = 'Killed' THEN 1
                            ELSE 0
                        END
                    ) AS TEXT
                )
            FROM
                Jobs
            WHERE
                date(run_at, 'unixepoch') = date('now')
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                3,
                'Decimal',
                'AVG_JOBS_PER_MINUTE_PAST_HOUR',
                CAST(ROUND(COUNT(*) / 60.0, 2) AS TEXT)
            FROM
                Jobs
            WHERE
                run_at >= strftime('%s', 'now', '-1 hour')
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                4,
                'Number',
                'TOTAL_JOBS',
                CAST(COUNT(*) AS TEXT)
            FROM
                Jobs
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                4,
                'Number',
                'DONE_JOBS',
                CAST(
                    SUM(
                        CASE
                            WHEN status = 'Done' THEN 1
                            ELSE 0
                        END
                    ) AS TEXT
                )
            FROM
                Jobs
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                4,
                'Number',
                'KILLED_JOBS',
                CAST(
                    SUM(
                        CASE
                            WHEN status = 'Killed' THEN 1
                            ELSE 0
                        END
                    ) AS TEXT
                )
            FROM
                Jobs
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                4,
                'Percentage',
                'SUCCESS_RATE',
                CAST(
                    ROUND(
                        100.0 * SUM(
                            CASE
                                WHEN status = 'Done' THEN 1
                                ELSE 0
                            END
                        ) / NULLIF(COUNT(*), 0),
                        2
                    ) AS TEXT
                )
            FROM
                Jobs
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                5,
                'Decimal',
                'AVG_JOB_DURATION_MINS',
                CAST(ROUND(AVG((done_at - run_at) / 60.0), 2) AS TEXT)
            FROM
                Jobs
            WHERE
                status IN ('Done', 'Failed', 'Killed')
                AND done_at IS NOT NULL
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                5,
                'Decimal',
                'LONGEST_RUNNING_JOB_MINS',
                CAST(
                    ROUND(
                        MAX(
                            CASE
                                WHEN status = 'Running' THEN (strftime('%s', 'now') - run_at) / 60.0
                                ELSE 0
                            END
                        ),
                        2
                    ) AS TEXT
                )
            FROM
                Jobs
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                6,
                'Number',
                'JOBS_PAST_7_DAYS',
                CAST(COUNT(*) AS TEXT)
            FROM
                Jobs
            WHERE
                run_at >= strftime('%s', 'now', '-7 days')
            GROUP BY
                job_type
            UNION
            ALL
            SELECT
                job_type,
                8,
                'Timestamp',
                'MOST_RECENT_JOB',
                CAST(MAX(run_at) AS TEXT)
            FROM
                Jobs
            GROUP BY
                job_type
            ORDER BY
                job_type,
                priority,
                statistic
        )
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
        Jobs
)
SELECT
    jt.job_type as name,
    COALESCE(qs.stats, '[]') as stats,
    COALESCE(
        (
            SELECT
                json_group_array(DISTINCT lock_by)
            FROM
                Jobs
            WHERE
                job_type = jt.job_type
                AND lock_by IS NOT NULL
        ),
        '[]'
    ) as workers,
    COALESCE(
        (
            SELECT
                json_group_array(daily_count)
            FROM
                (
                    SELECT
                        COUNT(*) as daily_count
                    FROM
                        Jobs
                    WHERE
                        job_type = jt.job_type
                        AND run_at >= strftime('%s', 'now', '-7 days')
                    GROUP BY
                        date(run_at, 'unixepoch')
                    ORDER BY
                        date(run_at, 'unixepoch')
                )
        ),
        '[]'
    ) as activity
FROM
    all_job_types jt
    LEFT JOIN queue_stats qs ON jt.job_type = qs.job_type
ORDER BY
    name;
