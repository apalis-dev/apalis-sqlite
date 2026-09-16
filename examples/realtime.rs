use std::time::Duration;

use apalis::{layers::tracing::info, prelude::*};
use apalis_sqlite::{Config, SqliteStorage};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    use tracing_subscriber::{EnvFilter, fmt};
    let fmt_layer = fmt::layer();
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("debug,sqlx=off"))
        .unwrap();

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let config = Config::default().queue("realtime-queue").lock_tasks(false);
    let (pool, listener) = SqliteStorage::connect_with_callback(":memory:").unwrap();
    SqliteStorage::setup(&pool).await.unwrap();

    let backend = SqliteStorage::new(&pool)
        .with_config(config)
        .poll_with_stream(listener);

    let mut b = backend.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let mut iters = 0;
        loop {
            iters += 1;
            tokio::time::sleep(Duration::from_millis(1000)).await;
            b.push(iters).await.unwrap();
            if iters == 10 {
                break;
            }
        }
    });

    async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
        if task == 10 {
            worker.stop()?;
        }
        Ok(())
    }
    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|_, ev| info!("{ev:?}"))
        .build(task);
    worker.run().await.unwrap();
}
