# apalis-sqlite

Background task processing for Rust using Apalis and SQLite.

## Features

- **Reliable job queue** using SQLite as the backend.
- **Multiple storage types**: standard polling and event-driven (hooked) storage.
- **Custom codecs** for serializing/deserializing job arguments as bytes and json.
- **Heartbeat and orphaned job re-enqueueing** for robust job processing.
- **Integration with Apalis workers and middleware.**

## Storage Types

- [`SqliteStorage`]: Standard polling-based storage.
- [`SqliteStorageWithHook`]: Event-driven storage using SQLite update hooks for low-latency job fetching.
- [`SharedSqliteStorage`]: Shared storage for multiple job types.

The naming is designed to clearly indicate the storage mechanism and its capabilities, but under the hood the result is the `SqliteStorage` struct with different configurations.

## Examples

### Basic Worker Example

```rust,no_run
#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();
    let mut backend = SqliteStorage::new(&pool); // With default config

    let mut start = 0;
    let mut items = stream::repeat_with(move || {
        start += 1;
        let task = Task::builder(start)
            .run_after(Duration::from_secs(1))
            .with_ctx(SqliteContext::new().with_priority(1))
            .build();
        Ok(task)
    })
    .take(10);
    backend.send_all(&mut items).await.unwrap();

    async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
        Ok(())
    }

    let worker = WorkerBuilder::new("worker-1")
        .backend(backend)
        .build(send_reminder);
    worker.run().await.unwrap();
}
```

### Hooked Worker Example (Event-driven)

```rust,no_run

#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();

    let lazy_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_secs(5)))
        .build();
    let config = Config::new("queue")
        .with_poll_interval(lazy_strategy)
        .set_buffer_size(5);
    let backend = SqliteStorage::new_with_callback(&pool, &config).await;

    tokio::spawn({
        let pool = pool.clone();
        let config = config.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut start = 0;
            let items = stream::repeat_with(move || {
                start += 1;
                Task::builder(serde_json::to_value(&start).unwrap())
                    .run_after(Duration::from_secs(1))
                    .with_ctx(SqliteContext::new().with_priority(start))
                    .build()
            })
            .take(20)
            .collect::<Vec<_>>()
            .await;
            apalis_sqlite::sink::push_tasks(pool, config, items).await.unwrap();
        }
    });

    async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
        Ok(())
    }

    let worker = WorkerBuilder::new("worker-2")
        .backend(backend)
        .build(send_reminder);
    worker.run().await.unwrap();
}
```

### Workflow Example

```rust,no_run
#[tokio::main]
async fn main() {
    let workflow = WorkFlow::new("odd-numbers-workflow")
        .then(|a: usize| async move {
            Ok::<_, WorkflowError>((0..=a).collect::<Vec<_>>())
        })
        .filter_map(|x| async move {
            if x % 2 != 0 { Some(x) } else { None }
        })
        .filter_map(|x| async move {
            if x % 3 != 0 { Some(x) } else { None }
        })
        .filter_map(|x| async move {
            if x % 5 != 0 { Some(x) } else { None }
        })
        .delay_for(Duration::from_millis(1000))
        .then(|a: Vec<usize>| async move {
            println!("Sum: {}", a.iter().sum::<usize>());
            Ok::<(), WorkflowError>(())
        });

    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();
    let mut sqlite = SqliteStorage::new_in_queue(&pool, "test-workflow");

    sqlite.push(100usize).await.unwrap();

    let worker = WorkerBuilder::new("rango-tango")
        .backend(sqlite)
        .on_event(|ctx, ev| {
            println!("On Event = {:?}", ev);
            if matches!(ev, Event::Error(_)) {
                ctx.stop().unwrap();
            }
        })
        .build(workflow);

    worker.run().await.unwrap();
}
```

## Migrations

If the `migrate` feature is enabled, you can run built-in migrations with:

```rust,no_run
use sqlx::SqlitePool;
#[tokio::main] async fn main() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    apalis_sqlite::SqliteStorage::setup(&pool).await.unwrap();
}
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
