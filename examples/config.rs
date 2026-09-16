use std::time::Duration;

use apalis::{config::WorkerConfig, config::WorkerFromConfig, prelude::*};
use apalis_sqlite::SqliteStorage;
use sqlx::Row;

const CONFIG: &str = r#"
{
  "name": "simple-worker",
  "backend": {
    "queue": "example-queue",
    "lock_task": false,
    "database_url": ":memory:"
  },
  "middleware": [
    "CatchPanic",
    "Tracing"
  ]
}
"#;

type Config = WorkerConfig<SqliteStorage<u32>>;

async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
    apalis_core::timer::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(task, 42);
    worker.stop()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let config: Config = serde_json::from_str(CONFIG).unwrap();

    let worker = WorkerBuilder::try_config(config)?
        .map_backend(|backend| {
            backend
                .after_start(|b| {
                    let mut b = b.clone();
                    async move {
                        b.push(42).await.unwrap();
                        Ok(())
                    }
                })
                .after_stop(|b| {
                    let pool = b.pool().clone();
                    async move {
                        let jobs_count = sqlx::query("SELECT count(*) FROM Jobs")
                            .fetch_one(&pool)
                            .await?;
                        assert_eq!(
                            jobs_count.get::<i64, _>(0),
                            1,
                            "There should be only one job"
                        );
                        Ok(())
                    }
                })
                .poll_with_interval(Duration::from_millis(100))
        })
        .build(task);

    worker.run().await?;

    Ok(())
}
