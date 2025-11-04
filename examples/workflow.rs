use apalis::prelude::*;
use apalis_sqlite::SqliteStorage;
use apalis_workflow::{TaskFlowSink, Workflow};
use sqlx::SqlitePool;

#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();
    let mut backend = SqliteStorage::new(&pool);
    backend.push_start(42).await.unwrap();

    async fn task1(task: u32) -> u32 {
        task + 99
    }
    async fn task2(task: u32) -> u32 {
        task + 1
    }
    async fn task3(task: u32, worker: WorkerContext) {
        assert_eq!(task, 142);
        worker.stop().unwrap();
    }
    let workflow = Workflow::new("test_workflow")
        .then(task1)
        .then(task2)
        .then(task3);
    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .build(workflow);
    worker.run().await.unwrap();
}
