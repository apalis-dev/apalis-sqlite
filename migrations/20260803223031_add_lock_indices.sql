CREATE INDEX IF NOT EXISTS idx_jobs_claimable ON Jobs(status, attempts)
WHERE
    status IN ('Queued', 'Pending', 'Failed');
