ALTER TABLE
    Jobs
ADD
    COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX idx_jobs_idempotency_key ON Jobs(job_type, idempotency_key);
