CREATE INDEX IF NOT EXISTS idx_jobs_status_type_recent
    ON
        Jobs (
            status,
            job_type,
            done_at DESC,
            run_at DESC
        );

CREATE INDEX IF NOT EXISTS idx_jobs_status_recent
    ON
        Jobs (
            status,
            done_at DESC,
            run_at DESC
        );
