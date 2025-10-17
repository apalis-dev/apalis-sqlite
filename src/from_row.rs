use std::str::FromStr;

use apalis_core::{
    backend::codec::Codec,
    task::{attempt::Attempt, builder::TaskBuilder, status::Status, task_id::TaskId},
};

use crate::{CompactType, SqliteTask, context::SqliteContext};

#[derive(Debug)]
pub(crate) struct TaskRow {
    pub(crate) job: CompactType,
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
    // pub(crate) meta: Option<Map<String, Value>>,
}

impl TaskRow {
    pub fn try_into_task<D: Codec<Args, Compact = CompactType>, Args>(
        self,
    ) -> Result<SqliteTask<Args>, sqlx::Error>
    where
        D::Error: std::error::Error + Send + Sync + 'static,
    {
        let ctx = SqliteContext::default()
            .with_done_at(self.done_at)
            .with_lock_by(self.lock_by)
            .with_max_attempts(self.max_attempts.unwrap_or(25) as i32)
            .with_last_result(self.last_result)
            .with_priority(self.priority.unwrap_or(0) as i32)
            .with_queue(
                self.job_type
                    .ok_or(sqlx::Error::ColumnNotFound("job_type".to_owned()))?,
            )
            .with_lock_at(self.lock_at);
        let args = D::decode(&self.job).map_err(|e| sqlx::Error::Decode(e.into()))?;
        let task = TaskBuilder::new(args)
            .with_ctx(ctx)
            .with_attempt(Attempt::new_with_value(
                self.attempts
                    .ok_or(sqlx::Error::ColumnNotFound("attempts".to_owned()))?
                    as usize,
            ))
            .with_status(
                self.status
                    .ok_or(sqlx::Error::ColumnNotFound("status".to_owned()))
                    .and_then(|s| {
                        Status::from_str(&s).map_err(|e| sqlx::Error::Decode(e.into()))
                    })?,
            )
            .with_task_id(
                self.id
                    .ok_or(sqlx::Error::ColumnNotFound("task_id".to_owned()))
                    .and_then(|s| {
                        TaskId::from_str(&s).map_err(|e| sqlx::Error::Decode(e.into()))
                    })?,
            )
            .run_at_timestamp(
                self.run_at
                    .ok_or(sqlx::Error::ColumnNotFound("run_at".to_owned()))?
                    as u64,
            );
        Ok(task.build())
    }
    pub fn try_into_task_compact(self) -> Result<SqliteTask<CompactType>, sqlx::Error> {
        let ctx = SqliteContext::default()
            .with_done_at(self.done_at)
            .with_lock_by(self.lock_by)
            .with_max_attempts(self.max_attempts.unwrap_or(25) as i32)
            .with_last_result(self.last_result)
            .with_priority(self.priority.unwrap_or(0) as i32)
            .with_queue(
                self.job_type
                    .ok_or(sqlx::Error::ColumnNotFound("job_type".to_owned()))?,
            )
            .with_lock_at(self.lock_at);

        let task = TaskBuilder::new(self.job)
            .with_ctx(ctx)
            .with_attempt(Attempt::new_with_value(
                self.attempts
                    .ok_or(sqlx::Error::ColumnNotFound("attempts".to_owned()))?
                    as usize,
            ))
            .with_status(
                self.status
                    .ok_or(sqlx::Error::ColumnNotFound("status".to_owned()))
                    .and_then(|s| {
                        Status::from_str(&s).map_err(|e| sqlx::Error::Decode(e.into()))
                    })?,
            )
            .with_task_id(
                self.id
                    .ok_or(sqlx::Error::ColumnNotFound("task_id".to_owned()))
                    .and_then(|s| {
                        TaskId::from_str(&s).map_err(|e| sqlx::Error::Decode(e.into()))
                    })?,
            )
            .run_at_timestamp(
                self.run_at
                    .ok_or(sqlx::Error::ColumnNotFound("run_at".to_owned()))?
                    as u64,
            );
        Ok(task.build())
    }
}
