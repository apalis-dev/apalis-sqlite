use apalis::prelude::*;
use apalis_sqlite::{Config, SqliteStorage};

#[tokio::main]
async fn main() {
    let config = Config::new("realtime-queue");
    let mut backend = SqliteStorage::new_with_callback(":memory:", &config);

    let pool = backend.pool();
    SqliteStorage::setup(&pool).await.unwrap();

    backend.push(42).await.unwrap();

    async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
        apalis_core::timer::sleep(std::time::Duration::from_secs(1)).await;
        assert_eq!(task, 42);
        worker.stop()?;
        Ok(())
    }
    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .build(task);
    worker.run().await.unwrap();
}
