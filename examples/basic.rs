use apalis::prelude::*;
use apalis_sqlite::SqliteStorage;
use sqlx::SqlitePool;

#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();
    let mut backend = SqliteStorage::new(&pool);
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
