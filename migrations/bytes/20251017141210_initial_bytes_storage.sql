CREATE TABLE IF NOT EXISTS Workers (
    id TEXT NOT NULL UNIQUE,
    worker_type TEXT NOT NULL,
    storage_name TEXT NOT NULL,
    layers TEXT,
    last_seen INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX IF NOT EXISTS Idx ON Workers(id);

CREATE INDEX IF NOT EXISTS WTIdx ON Workers(worker_type);

CREATE INDEX IF NOT EXISTS LSIdx ON Workers(last_seen);

CREATE TABLE IF NOT EXISTS Jobs (
    job BLOB NOT NULL,
    id TEXT NOT NULL UNIQUE,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 25,
    run_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    last_result TEXT,
    lock_at INTEGER,
    lock_by TEXT,
    done_at INTEGER,
    FOREIGN KEY(lock_by) REFERENCES Workers(id)
);

CREATE INDEX IF NOT EXISTS TIdx ON Jobs(id);

CREATE INDEX IF NOT EXISTS SIdx ON Jobs(status);

CREATE INDEX IF NOT EXISTS LIdx ON Jobs(lock_by);

CREATE INDEX IF NOT EXISTS JTIdx ON Jobs(job_type);


ALTER TABLE Jobs
ADD priority INTEGER NOT NULL DEFAULT 0;

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
