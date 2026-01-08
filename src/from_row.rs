use apalis_sql::{DateTime, DateTimeExt, TaskRow};

#[derive(Debug)]
pub(crate) struct SqliteTaskRow {
    pub(crate) job: Vec<u8>,
    pub(crate) id: Option<String>,
    pub(crate) job_type: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) attempts: Option<i64>,
    pub(crate) max_attempts: Option<i64>,
    pub(crate) run_at: Option<i64>,
    pub(crate) last_result: Option<String>,
    pub(crate) lock_at: Option<i64>,
    pub(crate) lock_by: Option<String>,
    pub(crate) done_at: Option<i64>,
    pub(crate) priority: Option<i64>,
    pub(crate) metadata: Option<String>,
}

impl TryInto<TaskRow> for SqliteTaskRow {
    type Error = sqlx::Error;

    fn try_into(self) -> Result<TaskRow, Self::Error> {
        Ok(TaskRow {
            job: self.job,
            id: self
                .id
                .ok_or_else(|| sqlx::Error::Protocol("Missing id".into()))?,
            job_type: self
                .job_type
                .ok_or_else(|| sqlx::Error::Protocol("Missing job_type".into()))?,
            status: self
                .status
                .ok_or_else(|| sqlx::Error::Protocol("Missing status".into()))?,
            attempts: self
                .attempts
                .ok_or_else(|| sqlx::Error::Protocol("Missing attempts".into()))?
                as usize,
            max_attempts: self.max_attempts.map(|v| v as usize),
            run_at: self
                .run_at
                .map(<DateTime as DateTimeExt>::from_unix_timestamp),
            last_result: self
                .last_result
                .map(|res| serde_json::from_str(&res).unwrap_or(serde_json::Value::Null)),
            lock_at: self
                .lock_at
                .map(<DateTime as DateTimeExt>::from_unix_timestamp),
            lock_by: self.lock_by,
            done_at: self
                .done_at
                .map(<DateTime as DateTimeExt>::from_unix_timestamp),
            priority: self.priority.map(|v| v as usize),
            metadata: self
                .metadata
                .map(|meta| serde_json::from_str(&meta).unwrap_or(serde_json::Value::Null)),
        })
    }
}
