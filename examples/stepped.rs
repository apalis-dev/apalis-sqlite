use std::time::Duration;

use apalis::prelude::*;
use apalis_sqlite::SqliteStorage;
use apalis_workflow::*;
use sqlx::SqlitePool;

#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();
    let mut backend = SqliteStorage::new(&pool);
    backend.push_start(42).await.unwrap();

    async fn task1(task: u32) -> String {
        println!("Executing task1 with input: {}", task);
        (task + 99).to_string()
    }
    async fn task2(task: String) -> u32 {
        println!("Executing task2 with input: {}", task);
        task.parse::<u32>().unwrap() + 1
    }
    async fn task3(task: u32, worker: WorkerContext) {
        println!("Executing task3 with input: {}", task);
        assert_eq!(task, 142);
        worker.stop().unwrap();
    }
    let workflow = Workflow::new("test_workflow")
        .and_then(task1)
        .delay_for(Duration::from_secs(1))
        .and_then(task2)
        .and_then(task3);
    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|_c, e| {
            println!("{e:?},");
        })
        .build(workflow);
    worker.run().await.unwrap();
}
