use apalis_core::worker::context::WorkerContext;
use sqlx::Executor;

use crate::config::Config;

/// Send a keep-alive signal to the database to indicate that the worker is still active
pub async fn keep_alive<'a, E>(
    executor: E,
    config: &Config,
    worker: &WorkerContext,
) -> Result<(), sqlx::Error>
where
    E: Executor<'a, Database = sqlx::Sqlite>,
{
    let worker = worker.name().to_owned();
    let queue = config.queue.to_string();
    let res = sqlx::query_file!("queries/backend/keep_alive.sql", worker, queue)
        .execute(executor)
        .await?;
    if res.rows_affected() == 0 {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "WORKER_DOES_NOT_EXIST",
        )));
    }
    Ok(())
}
