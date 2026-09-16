use std::time::{SystemTime, UNIX_EPOCH};

use apalis_core::{task::Task, worker::context::WorkerContext};
use sqlx::Executor;

use crate::{Error, config::Config, from_row::SqliteTaskRow};

/// Fetch the next batch of tasks from the sqlite backend
pub async fn fetch_next<'a, E>(
    executor: E,
    config: &Config,
    worker: &WorkerContext,
) -> Result<Vec<Task<Vec<u8>>>, Error>
where
    E: Executor<'a, Database = sqlx::Sqlite>,
{
    let job_type = config.queue.as_ref();
    let buffer_size = config.batch_size as i32;
    let worker = worker.name();
    let now: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    sqlx::query_file_as!(
        SqliteTaskRow,
        "queries/backend/fetch_next.sql",
        worker,
        job_type,
        buffer_size,
        now
    )
    .fetch_all(executor)
    .await?
    .into_iter()
    .map(|r| r.try_into())
    .collect()
}
