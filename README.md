# apalis-sqlite

[![Crates.io](https://img.shields.io/crates/v/apalis-sqlite.svg)](https://crates.io/crates/apalis-sqlite)
[![Docs.rs](https://docs.rs/apalis-sqlite/badge.svg)](https://docs.rs/apalis-sqlite)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20%2F%20Apache--2.0-blue.svg)](#license)
[![Build Status](https://img.shields.io/github/actions/workflow/status/apalis-dev/apalis-sqlite/ci.yml?branch=main)](https://github.com/apalis-dev/apalis-sqlite/actions)

> [Background task processing for Rust](https://apalis.dev/), powered by [`apalis`](https://crates.io/crates/apalis) and [SQLite](https://www.sqlite.org/).

---

## Features

- **Reliable job queue** — SQLite-backed persistence survives process restarts; no external broker needed.
- **Durable execution** — jobs are written to disk before processing begins, so a crash mid-flight won't silently drop work.
- **Automatic retries** — configure `max_attempts` per task; failed jobs are re-enqueued with backoff and never lost.
- **Heartbeat & orphan recovery** — workers emit periodic heartbeats; jobs held by a dead worker are automatically re-enqueued.
- **Scheduled & delayed jobs** — use `run_after` to enqueue work that should only execute at or after a future point in time.
- **Priority queues** — assign integer priorities so high-urgency jobs are always picked up first.
- **Multiple polling strategies** — choose between standard polling and event-driven (hooked) storage to trade latency for CPU usage.
- **Multi-step workflows** — chain async steps into pipelines with `apalis-workflow`; each stage only runs if the previous one succeeds.
- **Shared storage** — multiplex multiple job types over a single SQLite connection.
- **Custom codecs** — pluggable serialization/deserialization of job payloads as raw bytes.
- **First-class `apalis` integration** — works seamlessly with workers, tower layers, and middleware.

---

## Storage Types

| Type                                                                                                            | Description                                                                  |
| --------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| [`SqliteStorage`](https://docs.rs/apalis-sqlite/latest/apalis_sqlite/struct.SqliteStorage.html)                 | Standard polling-based storage.                                              |
| [`SqliteStorageWithHook`](https://docs.rs/apalis-sqlite/latest/apalis_sqlite/struct.SqliteStorageWithHook.html) | Event-driven storage using SQLite update hooks for low-latency job fetching. |
| [`SharedSqliteStorage`](https://docs.rs/apalis-sqlite/latest/apalis_sqlite/struct.SharedSqliteStorage.html)     | Shared storage supporting multiple job types over a single connection.       |

> All types are built on top of `SqliteStorage` with different configurations applied under the hood.

---

## Examples

### Basic Worker

Set up a pool, push jobs, and run a worker:

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
        Task::builder(start)
            .run_after(Duration::from_secs(1))
            .priority(1)
            .max_attempts(5)
            .build()
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

### Event-Driven Worker (Hooked)

Use SQLite update hooks to react to new jobs with minimal latency:

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
                    .priority(start)
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

### Sequential Workflow (Order Fulfilment Pipeline)

Chain async steps to model a real-world order processing pipeline — validating payment,
reserving inventory, dispatching a shipment notification, and emailing the customer.
Each stage only runs if the previous one succeeds; a failure at any step returns an
error and the job can be retried from the beginning.

```rust,no_run
use std::time::Duration;

use apalis::prelude::*;
use apalis_sqlite::*;
use apalis_workflow::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: u64,
    customer_email: String,
    items: Vec<String>,
    total_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChargedOrder {
    order_id: u64,
    customer_email: String,
    transaction_id: String,
    items: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StockedItem {
    order_id: u64,
    sku: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DispatchedOrder {
    order_id: u64,
    customer_email: String,
    tracking_number: String,
    item_count: usize,
}

/// Simulates an inventory check — returns `None` for out-of-stock SKUs so
async fn check_stock(item: StockedItem) -> Option<StockedItem> {
    let out_of_stock = ["WIDGET-C", "WIDGET-D"];
    if out_of_stock.contains(&item.sku.as_str()) {
        eprintln!("[{}] {} is out of stock — skipping", item.order_id, item.sku);
        None
    } else {
        Some(item)
    }
}

#[tokio::main]
async fn main() {
    let workflow = Workflow::new("order-fulfilment")
        // Step 1: Charge payment and return a record of the charged order.
        .and_then(|order: Order| async move {
            println!(
                "[{}] Charging ${:.2} for {} item(s)...",
                order.id,
                order.total_cents as f64 / 100.0,
                order.items.len(),
            );
            // Call your payment provider (e.g. Stripe) here.
            Ok::<ChargedOrder, BoxDynError>(ChargedOrder {
                order_id: order.id,
                customer_email: order.customer_email,
                transaction_id: format!("txn_{}", order.id),
                items: order.items,
            })
        })
        .and_then(|charged: ChargedOrder| async move {
            let stocked = charged
                .items
                .into_iter()
                .map(|sku| StockedItem { order_id: charged.order_id, sku })
                .collect::<Vec<_>>();
            Ok::<(ChargedOrder, Vec<StockedItem>), BoxDynError>((
                ChargedOrder { items: vec![], ..charged },
                stocked,
            ))
        })
        .and_then(|(charged, items): (ChargedOrder, Vec<StockedItem>)| async move {
            let available = futures::future::join_all(items.into_iter().map(check_stock))
                .await
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

            if available.is_empty() {
                return Err("No items available to fulfil this order".into());
            }

            println!(
                "[{}] {} item(s) confirmed in stock",
                charged.order_id,
                available.len()
            );
            Ok::<(ChargedOrder, Vec<StockedItem>), BoxDynError>((charged, available))
        })
        // Step 3: Brief pause before handing off to the courier API.
        .delay_for(Duration::from_millis(500))
        // Step 4: Dispatch the shipment and obtain a tracking number.
        .and_then(|(charged, items): (ChargedOrder, Vec<StockedItem>)| async move {
            let tracking_number = format!("TRACK-{}", charged.order_id);
            println!(
                "[{}] Dispatched {} item(s) — tracking: {}",
                charged.order_id,
                items.len(),
                tracking_number,
            );
            // Call your courier API (e.g. EasyPost, Shippo) here.
            Ok::<DispatchedOrder, BoxDynError>(DispatchedOrder {
                order_id: charged.order_id,
                customer_email: charged.customer_email,
                tracking_number,
                item_count: items.len(),
            })
        })
        // Step 5: Notify the customer by email.
        .and_then(|dispatched: DispatchedOrder| async move {
            println!(
                "[{}] Emailing {} — your {} item(s) are on the way! ({})",
                dispatched.order_id,
                dispatched.customer_email,
                dispatched.item_count,
                dispatched.tracking_number,
            );
            // Send a transactional email (e.g. Resend, SendGrid) here.
            Ok::<(), BoxDynError>(())
        });

    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();
    let mut sqlite = SqliteStorage::new_in_queue(&pool, "order-fulfilment");

    sqlite
        .push_start(Order {
            id: 1001,
            customer_email: "alice@example.com".into(),
            items: vec!["WIDGET-A".into(), "WIDGET-B".into(), "WIDGET-C".into()],
            total_cents: 7499,
        })
        .await
        .unwrap();

    let worker = WorkerBuilder::new("fulfilment-worker")
        .backend(sqlite)
        .on_event(|ctx, ev| {
            println!("Event: {:?}", ev);
            if matches!(ev, Event::Error(_)) {
                ctx.stop().unwrap();
            }
        })
        .build(workflow);

    worker.run().await.unwrap();
}
```

### DAG Workflow

Chain async steps to model a real-world ETL pipeline which runs multiple steps concurrently, then collects the results.

```rust,no_run
use std::time::Duration;

use apalis::prelude::*;
use apalis_codec::msgpack::MsgPackCodec;
use apalis_sqlite::SqliteStorage;
use apalis_workflow::*;
use sqlx::SqlitePool;

async fn get_name(user_id: u32) -> Result<String, BoxDynError> {
    Ok(user_id.to_string())
}

async fn get_age(user_id: u32) -> Result<usize, BoxDynError> {
    tokio::time::sleep(Duration::from_millis(800)).await;
    Ok(user_id as usize + 20)
}

async fn get_address(user_id: u32) -> Result<usize, BoxDynError> {
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(user_id as usize + 100)
}

async fn collector(
    (name, age, address): (String, usize, usize),
    wrk: WorkerContext,
) -> Result<usize, BoxDynError> {
    let result = name.parse::<usize>()? + age + address;
    wrk.stop().unwrap();
    Ok(result)
}

#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();
    let mut backend = SqliteStorage::new(&pool).with_codec::<MsgPackCodec>();
    backend.start_fan_out(vec![42, 43, 44]).await.unwrap();

    let dag_flow = DagFlow::new("user-etl-workflow");
    let get_name = dag_flow.node(get_name);
    let get_age = dag_flow.node(get_age);
    let get_address = dag_flow.node(get_address);
    dag_flow
        .node(collector)
        .depends_on((&get_name, &get_age, &get_address)); // Order and types matters here

    dag_flow.validate().unwrap(); // Ensure DAG is valid

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|_c, e| {
            println!("{e:?},");
        })
        .build(dag_flow);
    worker.run().await.unwrap();
}
```

### Shared Storage (Multiple Job Types)

Run multiple job types over a single SQLite connection:

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

---

## Observability

Monitor and inspect your jobs visually using [**apalis-board**](https://github.com/apalis-dev/apalis-board):

![apalis-board task view](https://github.com/apalis-dev/apalis-board/raw/main/screenshots/task.png)

---

## Related Crates

- [`apalis`](https://github.com/apalis-dev/apalis) — the core worker and middleware framework.
- [`apalis-workflow`](https://docs.rs/apalis-workflow) — multi-step workflow support built on apalis.
- [`apalis-board`](https://github.com/apalis-dev/apalis-board) — web UI for monitoring jobs.

---

## Showcase and Examples

- [`ryot`](https://github.com/IgnisDa/ryot) - A self hosted platform for tracking various facets of your life - media, fitness and more.
- [`decomp.dev`](https://github.com/encounter/decomp.dev) - A video game decompilation tool
- [`actix-ntfy-service`](https://github.com/apalis-dev/apalis-board/tree/main/examples/actix-ntfy-service) - Basic example that shows how to publish notifications using apalis-sqlite

---

## License

Licensed under either of:

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
