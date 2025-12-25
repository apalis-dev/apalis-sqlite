# apalis-sqlite

Background task processing for Rust using `apalis` and `sqlite`.

## Features

- **Reliable job queue** using SQLite as the backend.
- **Multiple storage types**: standard polling and event-driven (hooked) storage.
- **Custom codecs** for serializing/deserializing job arguments as bytes.
- **Heartbeat and orphaned job re-enqueueing** for robust job processing.
- **Integration with `apalis` workers and middleware.**

## Storage Types

- [`SqliteStorage`]: Standard polling-based storage.
- [`SqliteStorageWithHook`]: Event-driven storage using SQLite update hooks for low-latency job fetching.
- [`SharedSqliteStorage`]: Shared storage for multiple job types.

The naming is designed to clearly indicate the storage mechanism and its capabilities, but under the hood the result is the `SqliteStorage` struct with different configurations.

## Examples

### Basic Worker Example

```rust,no_run
use std::time::Duration;

use apalis::prelude::*;
use apalis_sqlite::*;
use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();
    let mut backend = SqliteStorage::new(&pool);

    let mut start = 0;
    let mut items = stream::repeat_with(move || {
        start += 1;
        let task = Task::builder(start)
            .run_after(Duration::from_secs(1))
            .with_ctx(SqlContext::new().with_priority(1))
            .build();
        task
    })
    .take(10);
    backend.push_all(&mut items).await.unwrap();

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
use std::time::Duration;

use apalis::prelude::*;
use apalis_sqlite::*;
use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() {
    let lazy_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_secs(5)))
        .build();
    let config = Config::new("queue")
        .with_poll_interval(lazy_strategy)
        .set_buffer_size(5);
    let backend = SqliteStorage::new_with_callback(":memory:", &config);

    let pool = backend.pool();
    SqliteStorage::setup(&pool).await.unwrap();

    tokio::spawn({
        let pool = pool.clone();
        let config = config.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut start = 0;
            let items = stream::repeat_with(move || {
                start += 1;
                Task::builder(serde_json::to_vec(&start).unwrap())
                    .run_after(Duration::from_secs(1))
                    .with_ctx(SqlContext::new().with_priority(start))
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
use std::time::Duration;

use apalis::prelude::*;
use apalis_sqlite::*;
use apalis_workflow::*;

#[tokio::main]
async fn main() {
    let workflow = Workflow::new("odd-numbers-workflow")
        .and_then(|a: usize| async move {
            Ok::<Vec<usize>, BoxDynError>((0..=a).collect::<Vec<usize>>())
        })
        .filter_map(|x: usize| async move {
            if x % 2 != 0 { Some(x) } else { None }
        })
        .filter_map(|x: usize| async move {
            if x % 3 != 0 { Some(x) } else { None }
        })
        .filter_map(|x: usize| async move {
            if x % 5 != 0 { Some(x) } else { None }
        })
        .delay_for(Duration::from_millis(1000))
        .and_then(|a: Vec<usize>| async move {
            println!("Sum: {}", a.iter().sum::<usize>());
            Ok::<(), BoxDynError>(())
        });

    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();
    let mut sqlite = SqliteStorage::new_in_queue(&pool, "test-workflow");

    sqlite.push_start(100usize).await.unwrap();

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

### Shared Example

Full support for sharing the same connection. This example shows how to run multiple types with one function

```rust,no_run
use std::{collections::HashMap, time::Duration};

use apalis::prelude::*;
use apalis_sqlite::{SharedSqliteStorage, SqliteStorage};

use futures::stream;

#[tokio::main]
async fn main() {

    let mut store = SharedSqliteStorage::new(":memory:");

    SqliteStorage::setup(store.pool()).await.unwrap();

    let mut map_store = store.make_shared().unwrap();

    let mut int_store = store.make_shared().unwrap();

    map_store
        .push_stream(&mut stream::iter(vec![HashMap::<String, String>::new()]))
        .await
        .unwrap();
    int_store.push(99).await.unwrap();

    async fn send_reminder<T, I>(
        _: T,
        _task_id: TaskId<I>,
        wrk: WorkerContext,
    ) -> Result<(), BoxDynError> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        wrk.stop().unwrap();
        println!("Reminder sent!");
        Ok(())
    }

    let int_worker = WorkerBuilder::new("rango-tango-2")
        .backend(int_store)
        .build(send_reminder);
    let map_worker = WorkerBuilder::new("rango-tango-1")
        .backend(map_store)
        .build(send_reminder);
    tokio::try_join!(int_worker.run(), map_worker.run()).unwrap();
}
```

## Observability

You can track your jobs using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/main/screenshots/task.png)

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
