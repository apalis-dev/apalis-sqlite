use apalis_core::backend::TaskResult;
use sqlx::Executor;

/// Represents the result of a `SqliteTask` execution
pub type AckPayload = TaskResult<serde_json::Value>;

/// Ack multiple tasks, given a worker
pub async fn ack_tasks<'a, E>(
    executor: E,
    messages: &[&AckPayload],
    worker_id: &str,
) -> Result<u64, sqlx::Error>
where
    E: Executor<'a, Database = sqlx::Sqlite>,
{
    let payload_json =
        serde_json::to_string(&messages).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;

    let result = sqlx::query_file!("queries/task/ack.sql", payload_json, worker_id)
        .execute(executor)
        .await?;

    Ok(result.rows_affected())
}
