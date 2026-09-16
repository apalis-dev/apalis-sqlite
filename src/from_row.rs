use std::str::FromStr;

use apalis_core::task::{builder::TaskBuilder, status::Status, task_id::TaskId};
use ulid::Ulid;

use crate::{Error, SqliteTask};

#[derive(Debug)]
pub(crate) struct SqliteTaskRow {
    pub(crate) job: Vec<u8>,
    pub(crate) id: Option<String>,
    pub(crate) job_type: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) attempts: Option<i64>,
    pub(crate) max_attempts: Option<i64>,
    pub(crate) run_at: Option<i64>,
    pub(crate) lock_at: Option<i64>,
    pub(crate) lock_by: Option<String>,
    pub(crate) done_at: Option<i64>,
    pub(crate) priority: Option<i64>,
    pub(crate) metadata: Option<String>,
    pub(crate) idempotency_key: Option<String>,
    #[allow(unused)]
    pub(crate) last_result: Option<String>,
}

impl TryInto<SqliteTask> for SqliteTaskRow {
    type Error = Error;

    fn try_into(self) -> Result<SqliteTask, Self::Error> {
        let mut task = TaskBuilder::new(self.job)
            .task_id({
                let task_id = self
                    .id
                    .ok_or_else(|| sqlx::Error::ColumnNotFound("task_id".into()))?;
                TaskId::from_ulid(
                    Ulid::from_string(&task_id).map_err(|e| sqlx::Error::Decode(e.into()))?,
                )
            })
            .queue(
                self.job_type
                    .ok_or_else(|| sqlx::Error::ColumnNotFound("job_type".into()))?
                    .into(),
            )
            .status(
                Status::from_str(
                    &self
                        .status
                        .ok_or_else(|| sqlx::Error::ColumnNotFound("status".into()))?,
                )
                .map_err(|e| sqlx::Error::Decode(e.into()))?,
            )
            .attempt(
                self.attempts
                    .ok_or_else(|| sqlx::Error::ColumnNotFound("attempts".into()))?
                    as usize,
            )
            .max_attempts(self.max_attempts.map(|v| v as usize).unwrap_or(25))
            .run_at_timestamp(
                self.run_at
                    .ok_or(sqlx::Error::ColumnNotFound("run_at".to_owned()))?
                    as u64,
            )
            .lock_at(self.lock_at.map(|dt| dt as u64))
            .done_at(self.done_at.map(|dt| dt as u64))
            .lock_by(self.lock_by)
            .priority(self.priority.map(|v| v as usize).unwrap_or_default())
            .with_metadata(
                self.metadata
                    .map(|meta| serde_json::from_str(&meta).unwrap_or_default())
                    .unwrap_or_default(),
            );

        if let Some(idempotency_key) = self.idempotency_key {
            task = task.idempotency_key(idempotency_key);
        }

        Ok(task.build())
    }
}
