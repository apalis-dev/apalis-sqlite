use apalis_core::worker::context::WorkerContext;
use futures::{FutureExt, Stream, stream};
use sqlx::SqlitePool;

use crate::{
    Config,
    queries::{reenqueue_orphaned::reenqueue_orphaned, register_worker::register_worker},
};

/// Send a keep-alive signal to the database to indicate that the worker is still active
pub async fn keep_alive(
    pool: SqlitePool,
    config: Config,
    worker: WorkerContext,
) -> Result<(), sqlx::Error> {
    let worker = worker.name().to_owned();
    let queue = config.queue().to_string();
    let res = sqlx::query_file!("queries/backend/keep_alive.sql", worker, queue)
        .execute(&pool)
        .await?;
    if res.rows_affected() == 0 {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "WORKER_DOES_NOT_EXIST",
        )));
    }
    Ok(())
}

/// Perform the initial heartbeat and registration of the worker
pub async fn initial_heartbeat(
    pool: SqlitePool,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), sqlx::Error> {
    reenqueue_orphaned(pool.clone(), &config).await?;
    register_worker(pool, config, worker, storage_type).await?;
    Ok(())
}

/// Create a stream that sends keep-alive signals at regular intervals
pub fn keep_alive_stream(
    pool: SqlitePool,
    config: Config,
    worker: WorkerContext,
) -> impl Stream<Item = Result<(), sqlx::Error>> + Send {
    stream::unfold((), move |_| {
        let register = keep_alive(pool.clone(), config.clone(), worker.clone());
        let interval = apalis_core::timer::Delay::new(*config.keep_alive());
        interval.then(move |_| register.map(|res| Some((res, ()))))
    })
}
