use sqlx::{Executor, Sqlite};

/// Lock a task, given a worker
pub async fn lock_task<E: for<'a> Executor<'a, Database = Sqlite>>(
    pool: E,
    task_id: &str,
    worker_id: &str,
) -> Result<(), sqlx::Error> {
    let res = sqlx::query_file!("queries/task/lock.sql", task_id, worker_id)
        .execute(pool)
        .await?;

    if res.rows_affected() == 0 {
        return Err(sqlx::Error::RowNotFound);
    }
    Ok(())
}
