CREATE INDEX IF NOT EXISTS idx_jobs_job_type_status_run_at ON Jobs(job_type, status, run_at);

CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at ON Jobs(status, run_at);

CREATE INDEX IF NOT EXISTS idx_jobs_run_at_status ON Jobs(run_at, status);

CREATE INDEX IF NOT EXISTS idx_jobs_completed_done_at ON Jobs(status, done_at, run_at)
WHERE
    status IN ('Done', 'Failed', 'Killed')
    AND done_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_pending ON Jobs(run_at)
WHERE
    status = 'Pending';

CREATE INDEX IF NOT EXISTS idx_jobs_running ON Jobs(run_at)
WHERE
    status = 'Running';

CREATE INDEX IF NOT EXISTS idx_jobs_job_type_run_at ON Jobs(job_type, run_at);

CREATE INDEX IF NOT EXISTS idx_jobs_job_type_covering ON Jobs(job_type, status, run_at, done_at);
