use apalis_core::worker::context::WorkerContext;
use sqlx::SqlitePool;

use crate::Config;

pub async fn register_worker(
    pool: SqlitePool,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), sqlx::Error> {
    let worker_id = worker.name().to_owned();
    let queue = config.queue().to_string();
    let layers = worker.get_service().to_owned();
    let keep_alive = config.keep_alive().as_secs() as i64;
    let res = sqlx::query_file!(
        "queries/backend/register_worker.sql",
        worker_id,
        queue,
        storage_type,
        layers,
        keep_alive,
    )
    .execute(&pool)
    .await?;
    if res.rows_affected() == 0 {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "WORKER_ALREADY_EXISTS",
        )));
    }
    Ok(())
}
