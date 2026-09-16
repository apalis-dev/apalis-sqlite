use sqlx::Executor;

/// Lock multiple tasks, given a worker
pub async fn lock_tasks<'a, E>(
    executor: E,
    task_ids: &[String],
    worker_id: &str,
) -> Result<u64, sqlx::Error>
where
    E: Executor<'a, Database = sqlx::Sqlite>,
{
    let ids_json = serde_json::to_string(task_ids).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

    let res = sqlx::query_file!("queries/task/lock.sql", ids_json, worker_id)
        .execute(executor)
        .await?;

    Ok(res.rows_affected())
}
