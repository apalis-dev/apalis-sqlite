use apalis_core::{backend::WorkerFilter, task::task_id::coalesce_ids};
use sqlx::Executor;

/// Re-enqueue tasks that were being processed by dead workers
///
/// A worker that has not sent a keep-alive signal within the heartbeat duration is considered dead
pub async fn reenqueue_orphaned<'a, E>(
    executor: E,
    dead_for: i64,
    queue: &str,
    filter: &WorkerFilter,
) -> Result<u64, sqlx::Error>
where
    E: Executor<'a, Database = sqlx::Sqlite>,
{
    let (exclude_id, only_id) = match filter {
        WorkerFilter::AllExcept(id) => (Some(id), None),
        WorkerFilter::Only(id) => (None, Some(id)),
        WorkerFilter::None => (None, None),
        _ => unreachable!(),
    };
    match sqlx::query_file!(
        "queries/backend/reenqueue_orphaned.sql",
        dead_for,
        queue,
        exclude_id,
        only_id,
    )
    .execute(executor)
    .await
    {
        Ok(res) => Ok(res.rows_affected()),
        Err(e) => Err(e),
    }
}

/// Re-enqueue tasks that were being processed by dying a worker
///
/// This will be invoked during `Backend::poll_close`
pub async fn reenqueue_abandoned<'a, E>(
    executor: E,
    queue: &str,
    worker: &str,
    task_ids: &Vec<String>,
) -> Result<u64, sqlx::Error>
where
    E: Executor<'a, Database = sqlx::Sqlite>,
{
    let task_ids = coalesce_ids(task_ids);
    match sqlx::query_file!(
        "queries/backend/reenqueue_abandoned.sql",
        queue,
        worker,
        task_ids
    )
    .execute(executor)
    .await
    {
        Ok(res) => Ok(res.rows_affected()),
        Err(e) => Err(e),
    }
}
