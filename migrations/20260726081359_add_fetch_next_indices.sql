DROP INDEX IF EXISTS idx_jobs_pending;

CREATE INDEX IF NOT EXISTS idx_jobs_pending ON Jobs (job_type, priority DESC, run_at ASC, id ASC)
WHERE
    status = 'Pending'
    AND lock_by IS NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_failed ON Jobs (job_type, priority DESC, run_at ASC, id ASC)
WHERE
    status = 'Failed'
    AND attempts < max_attempts;
