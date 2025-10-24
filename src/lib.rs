//! # apalis-sqlite
//!
//! Background task processing for rust using apalis and sqlite.
//!
//! ## Features
//!
//! - **Reliable job queue** using SQLite as the backend.
//! - **Multiple storage types**: standard polling and event-driven (hooked) storage.
//! - **Custom codecs** for serializing/deserializing job arguments.
//! - **Heartbeat and orphaned job re-enqueueing** for robust job processing.
//! - **Integration with apalis workers and middleware.**
//!
//! ## Storage Types
//!
//! - [`SqliteStorage`]: Standard polling-based storage.
//! - [`SqliteStorageWithHook`]: Event-driven storage using SQLite update hooks for low-latency job fetching.
//! - [`SharedSqliteStorage`]: Shared storage for multiple job types.
//!
//! The naming is designed to clearly indicate the storage mechanism and its capabilities, but under the hood its the result is the `SqliteStorage` struct with different configurations.
//!
//! ## Examples
//!
//! ### Basic Worker Example
//!
//! ```rust
//! # use apalis_sqlite::{SqliteStorage, SqlContext};
//! # use apalis_core::task::Task;
//! # use apalis_core::worker::context::WorkerContext;
//! # use sqlx::SqlitePool;
//! # use futures::stream;
//! # use std::time::Duration;
//! # use apalis_core::error::BoxDynError;
//! # use futures::StreamExt;
//! # use futures::SinkExt;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::TaskSink;
//! #[tokio::main]
//! async fn main() {
//!     let pool = SqlitePool::connect(":memory:").await.unwrap();
//!     SqliteStorage::setup(&pool).await.unwrap();
//!     let mut backend = SqliteStorage::new(&pool);
//!
//!     let mut start = 0usize;
//!     let mut items = stream::repeat_with(move || {
//!         start += 1;
//!         start
//!     })
//!     .take(10);
//!     backend.push_stream(&mut items).await.unwrap();
//!
//!     async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
//!         if item == 10 {
//!             wrk.stop().unwrap();
//!         }
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("worker-1")
//!         .backend(backend)
//!         .build(send_reminder);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ### Hooked Worker Example (Event-driven)
//!
//! ```rust,no_run
//! # use apalis_sqlite::{SqliteStorage, SqlContext, Config};
//! # use apalis_core::task::Task;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::backend::poll_strategy::{IntervalStrategy, StrategyBuilder};
//! # use sqlx::SqlitePool;
//! # use futures::stream;
//! # use std::time::Duration;
//! # use apalis_core::error::BoxDynError;
//! # use futures::StreamExt;
//! # use futures::SinkExt;
//! # use apalis_core::worker::builder::WorkerBuilder;
//!
//! #[tokio::main]
//! async fn main() {
//!     let pool = SqlitePool::connect(":memory:").await.unwrap();
//!     SqliteStorage::setup(&pool).await.unwrap();
//!
//!     let lazy_strategy = StrategyBuilder::new()
//!         .apply(IntervalStrategy::new(Duration::from_secs(5)))
//!         .build();
//!     let config = Config::new("queue")
//!         .with_poll_interval(lazy_strategy)
//!         .set_buffer_size(5);
//!     let backend = SqliteStorage::new_with_callback(&pool, &config);
//!
//!     tokio::spawn({
//!         let pool = pool.clone();
//!         let config = config.clone();
//!         async move {
//!             tokio::time::sleep(Duration::from_secs(2)).await;
//!             let mut start = 0;
//!             let items = stream::repeat_with(move || {
//!                 start += 1;
//!                 Task::builder(serde_json::to_string(&start).unwrap())
//!                     .run_after(Duration::from_secs(1))
//!                     .with_ctx(SqlContext::new().with_priority(start))
//!                     .build()
//!             })
//!             .take(20)
//!             .collect::<Vec<_>>()
//!             .await;
//!             // push encoded tasks
//!             apalis_sqlite::sink::push_tasks(pool, config, items).await.unwrap();
//!         }
//!     });
//!
//!     async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
//!         if item == 1 {
//!             apalis_core::timer::sleep(Duration::from_secs(1)).await;
//!             wrk.stop().unwrap();
//!         }
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("worker-2")
//!         .backend(backend)
//!         .build(send_reminder);
//!     worker.run().await.unwrap();
//! }
//! ```
//! ### Workflow Example
//!
//! ```rust,no_run
//! # use apalis_sqlite::{SqliteStorage, SqlContext, Config};
//! # use apalis_core::task::Task;
//! # use apalis_core::worker::context::WorkerContext;
//! # use sqlx::SqlitePool;
//! # use futures::stream;
//! # use std::time::Duration;
//! # use apalis_core::error::BoxDynError;
//! # use futures::StreamExt;
//! # use futures::SinkExt;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_workflow::WorkFlow;
//! # use apalis_workflow::WorkflowError;
//! # use apalis_core::worker::event::Event;
//! # use apalis_core::backend::WeakTaskSink;
//! # use apalis_core::worker::ext::event_listener::EventListenerExt;
//! #[tokio::main]
//! async fn main() {
//!     let workflow = WorkFlow::new("odd-numbers-workflow")
//!         .then(|a: usize| async move {
//!             Ok::<_, WorkflowError>((0..=a).collect::<Vec<_>>())
//!         })
//!         .filter_map(|x| async move {
//!             if x % 2 != 0 { Some(x) } else { None }
//!         })
//!         .filter_map(|x| async move {
//!             if x % 3 != 0 { Some(x) } else { None }
//!         })
//!         .filter_map(|x| async move {
//!             if x % 5 != 0 { Some(x) } else { None }
//!         })
//!         .delay_for(Duration::from_millis(1000))
//!         .then(|a: Vec<usize>| async move {
//!             println!("Sum: {}", a.iter().sum::<usize>());
//!             Ok::<(), WorkflowError>(())
//!         });
//!
//!     let pool = SqlitePool::connect(":memory:").await.unwrap();
//!     SqliteStorage::setup(&pool).await.unwrap();
//!     let mut sqlite = SqliteStorage::new_in_queue(&pool, "test-workflow");
//!
//!     sqlite.push(100usize).await.unwrap();
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(sqlite)
//!         .on_event(|ctx, ev| {
//!             println!("On Event = {:?}", ev);
//!             if matches!(ev, Event::Error(_)) {
//!                 ctx.stop().unwrap();
//!             }
//!         })
//!         .build(workflow);
//!
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ## Migrations
//!
//! If the `migrate` feature is enabled, you can run built-in migrations with:
//!
//! ```rust,no_run
//! # use sqlx::SqlitePool;
//! # use apalis_sqlite::SqliteStorage;
//! # #[tokio::main] async fn main() {
//! let pool = SqlitePool::connect(":memory:").await.unwrap();
//! SqliteStorage::setup(&pool).await.unwrap();
//! # }
//! ```
//!
//! ## License
//!
//! Licensed under either of Apache License, Version 2.0 or MIT license at your option.
//!
//! [`SqliteStorageWithHook`]: crate::SqliteStorage
use std::{fmt, marker::PhantomData};

use apalis_core::{
    backend::{
        Backend, TaskStream,
        codec::{Codec, json::JsonCodec},
    },
    task::Task,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use apalis_sql::context::SqlContext;
use futures::{
    FutureExt, StreamExt, TryFutureExt, TryStreamExt,
    channel::mpsc,
    future::ready,
    stream::{self, BoxStream, select},
};
use libsqlite3_sys::{sqlite3, sqlite3_update_hook};
use sqlx::{Pool, Sqlite};
use std::ffi::c_void;
use tower_layer::Stack;
use ulid::Ulid;

use crate::{
    ack::{LockTaskLayer, SqliteAck},
    callback::{HookCallbackListener, update_hook_callback},
    fetcher::{SqliteFetcher, SqlitePollFetcher, fetch_next},
    queries::{
        keep_alive::{initial_heartbeat, keep_alive, keep_alive_stream},
        reenqueue_orphaned::reenqueue_orphaned_stream,
    },
    sink::SqliteSink,
};

mod ack;
mod callback;
mod config;
pub mod fetcher;
pub mod queries;
mod shared;
pub mod sink;

mod from_row {
    use chrono::{TimeZone, Utc};

    #[derive(Debug)]
    pub(crate) struct SqliteTaskRow {
        pub(crate) job: Vec<u8>,
        pub(crate) id: Option<String>,
        pub(crate) job_type: Option<String>,
        pub(crate) status: Option<String>,
        pub(crate) attempts: Option<i64>,
        pub(crate) max_attempts: Option<i64>,
        pub(crate) run_at: Option<i64>,
        pub(crate) last_result: Option<String>,
        pub(crate) lock_at: Option<i64>,
        pub(crate) lock_by: Option<String>,
        pub(crate) done_at: Option<i64>,
        pub(crate) priority: Option<i64>,
        pub(crate) metadata: Option<String>,
    }

    impl TryInto<apalis_sql::from_row::TaskRow> for SqliteTaskRow {
        type Error = sqlx::Error;

        fn try_into(self) -> Result<apalis_sql::from_row::TaskRow, Self::Error> {
            Ok(apalis_sql::from_row::TaskRow {
                job: self.job,
                id: self
                    .id
                    .ok_or_else(|| sqlx::Error::Protocol("Missing id".into()))?,
                job_type: self
                    .job_type
                    .ok_or_else(|| sqlx::Error::Protocol("Missing job_type".into()))?,
                status: self
                    .status
                    .ok_or_else(|| sqlx::Error::Protocol("Missing status".into()))?,
                attempts: self
                    .attempts
                    .ok_or_else(|| sqlx::Error::Protocol("Missing attempts".into()))?
                    as usize,
                max_attempts: self.max_attempts.map(|v| v as usize),
                run_at: self.run_at.map(|ts| {
                    Utc.timestamp_opt(ts, 0)
                        .single()
                        .ok_or_else(|| sqlx::Error::Protocol("Invalid run_at timestamp".into()))
                        .unwrap()
                }),
                last_result: self
                    .last_result
                    .map(|res| serde_json::from_str(&res).unwrap_or(serde_json::Value::Null)),
                lock_at: self.lock_at.map(|ts| {
                    Utc.timestamp_opt(ts, 0)
                        .single()
                        .ok_or_else(|| sqlx::Error::Protocol("Invalid run_at timestamp".into()))
                        .unwrap()
                }),
                lock_by: self.lock_by,
                done_at: self.done_at.map(|ts| {
                    Utc.timestamp_opt(ts, 0)
                        .single()
                        .ok_or_else(|| sqlx::Error::Protocol("Invalid run_at timestamp".into()))
                        .unwrap()
                }),
                priority: self.priority.map(|v| v as usize),
                metadata: self
                    .metadata
                    .map(|meta| serde_json::from_str(&meta).unwrap_or(serde_json::Value::Null)),
            })
        }
    }
}

pub type SqliteTask<Args> = Task<Args, SqlContext, Ulid>;
pub use callback::{CallbackListener, DbEvent};
pub use config::Config;
pub use shared::{SharedPostgresError, SharedSqliteStorage};
pub use sqlx::SqlitePool;

pub type CompactType = Vec<u8>;

const INSERT_OPERATION: &str = "INSERT";
const JOBS_TABLE: &str = "Jobs";

#[pin_project::pin_project]
pub struct SqliteStorage<T, C, Fetcher> {
    pool: Pool<Sqlite>,
    job_type: PhantomData<T>,
    config: Config,
    codec: PhantomData<C>,
    #[pin]
    sink: SqliteSink<T, CompactType, C>,
    #[pin]
    fetcher: Fetcher,
}

impl<T, C, F> fmt::Debug for SqliteStorage<T, C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStorage")
            .field("pool", &self.pool)
            .field("job_type", &"PhantomData<T>")
            .field("config", &self.config)
            .field("codec", &std::any::type_name::<C>())
            .finish()
    }
}

impl<T, C, F: Clone> Clone for SqliteStorage<T, C, F> {
    fn clone(&self) -> Self {
        SqliteStorage {
            sink: self.sink.clone(),
            pool: self.pool.clone(),
            job_type: PhantomData,
            config: self.config.clone(),
            codec: self.codec,
            fetcher: self.fetcher.clone(),
        }
    }
}

impl SqliteStorage<(), (), ()> {
    /// Perform migrations for storage
    #[cfg(feature = "migrate")]
    pub async fn setup(pool: &Pool<Sqlite>) -> Result<(), sqlx::Error> {
        sqlx::query("PRAGMA journal_mode = 'WAL';")
            .execute(pool)
            .await?;
        sqlx::query("PRAGMA temp_store = 2;").execute(pool).await?;
        sqlx::query("PRAGMA synchronous = NORMAL;")
            .execute(pool)
            .await?;
        sqlx::query("PRAGMA cache_size = 64000;")
            .execute(pool)
            .await?;
        Self::migrations().run(pool).await?;
        Ok(())
    }

    /// Get sqlite migrations without running them
    #[cfg(feature = "migrate")]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("./migrations")
    }
}

impl<T> SqliteStorage<T, (), ()> {
    /// Create a new SqliteStorage
    pub fn new(
        pool: &Pool<Sqlite>,
    ) -> SqliteStorage<
        T,
        JsonCodec<CompactType>,
        fetcher::SqliteFetcher<T, CompactType, JsonCodec<CompactType>>,
    > {
        let config = Config::new(std::any::type_name::<T>());
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            sink: SqliteSink::new(pool, &config),
            config,
            codec: PhantomData,
            fetcher: fetcher::SqliteFetcher {
                _marker: PhantomData,
            },
        }
    }

    pub fn new_in_queue(
        pool: &Pool<Sqlite>,
        queue: &str,
    ) -> SqliteStorage<
        T,
        JsonCodec<CompactType>,
        fetcher::SqliteFetcher<T, CompactType, JsonCodec<CompactType>>,
    > {
        let config = Config::new(queue);
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            sink: SqliteSink::new(pool, &config),
            config,
            codec: PhantomData,
            fetcher: fetcher::SqliteFetcher {
                _marker: PhantomData,
            },
        }
    }

    pub fn new_with_codec<Codec>(
        pool: &Pool<Sqlite>,
        config: &Config,
    ) -> SqliteStorage<T, Codec, fetcher::SqliteFetcher<T, CompactType, Codec>> {
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            config: config.clone(),
            codec: PhantomData,
            sink: SqliteSink::new(pool, config),
            fetcher: fetcher::SqliteFetcher {
                _marker: PhantomData,
            },
        }
    }

    pub fn new_with_config(
        pool: &Pool<Sqlite>,
        config: &Config,
    ) -> SqliteStorage<
        T,
        JsonCodec<CompactType>,
        fetcher::SqliteFetcher<T, CompactType, JsonCodec<CompactType>>,
    > {
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            config: config.clone(),
            codec: PhantomData,
            sink: SqliteSink::new(pool, config),
            fetcher: fetcher::SqliteFetcher {
                _marker: PhantomData,
            },
        }
    }

    pub fn new_with_callback(
        pool: &Pool<Sqlite>,
        config: &Config,
    ) -> SqliteStorage<T, JsonCodec<CompactType>, HookCallbackListener> {
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            config: config.clone(),
            codec: PhantomData,
            sink: SqliteSink::new(pool, config),
            fetcher: HookCallbackListener,
        }
    }

    pub fn new_with_codec_callback<Codec>(
        pool: &Pool<Sqlite>,
        config: &Config,
    ) -> SqliteStorage<T, Codec, HookCallbackListener> {
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            config: config.clone(),
            codec: PhantomData,
            sink: SqliteSink::new(pool, config),
            fetcher: HookCallbackListener,
        }
    }
}

impl<T, C, F> SqliteStorage<T, C, F> {
    pub fn config(&self) -> &Config {
        &self.config
    }
}

impl<Args, Decode> Backend for SqliteStorage<Args, Decode, SqliteFetcher<Args, CompactType, Decode>>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + 'static + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;

    type Context = SqlContext;

    type Codec = Decode;

    type Compact = CompactType;

    type Error = sqlx::Error;

    type Stream = TaskStream<SqliteTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<SqliteAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let keep_alive = keep_alive_stream(pool, config, worker);
        let reenqueue = reenqueue_orphaned_stream(
            self.pool.clone(),
            self.config.clone(),
            *self.config.keep_alive(),
        )
        .map_ok(|_| ());
        futures::stream::select(keep_alive, reenqueue).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        let lock = LockTaskLayer::new(self.pool.clone());
        let ack = AcknowledgeLayer::new(SqliteAck::new(self.pool.clone()));
        Stack::new(lock, ack)
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let fut = initial_heartbeat(
            self.pool.clone(),
            self.config().clone(),
            worker.clone(),
            "SqliteStorage",
        );
        let register = stream::once(fut.map(|_| Ok(None)));
        register
            .chain(SqlitePollFetcher::<Args, CompactType, Decode>::new(
                &self.pool,
                &self.config,
                worker,
            ))
            .boxed()
    }
}

impl<Args, Decode> Backend for SqliteStorage<Args, Decode, HookCallbackListener>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;

    type Context = SqlContext;

    type Codec = Decode;

    type Compact = CompactType;

    type Error = sqlx::Error;

    type Stream = TaskStream<SqliteTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<SqliteAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let keep_alive = keep_alive_stream(pool, config, worker);
        let reenqueue = reenqueue_orphaned_stream(
            self.pool.clone(),
            self.config.clone(),
            *self.config.keep_alive(),
        )
        .map_ok(|_| ());
        futures::stream::select(keep_alive, reenqueue).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        let lock = LockTaskLayer::new(self.pool.clone());
        let ack = AcknowledgeLayer::new(SqliteAck::new(self.pool.clone()));
        Stack::new(lock, ack)
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let (tx, rx) = mpsc::unbounded::<DbEvent>();

        let listener = CallbackListener::new(rx);

        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let register_worker = initial_heartbeat(
            self.pool.clone(),
            self.config.clone(),
            worker.clone(),
            "SqliteStorageWithHook",
        );
        let p = pool.clone();
        let register_worker = stream::once(
            register_worker
                .and_then(|_| async move {
                    // This is still a little tbd, but the idea is to test the update hook
                    let mut conn = p.acquire().await?;
                    // Get raw sqlite3* handle
                    let handle: *mut sqlite3 =
                        conn.lock_handle().await.unwrap().as_raw_handle().as_ptr();

                    // Put sender in a Box so it has a stable memory address
                    let tx_box = Box::new(tx);
                    let tx_ptr = Box::into_raw(tx_box) as *mut c_void;

                    unsafe {
                        sqlite3_update_hook(handle, Some(update_hook_callback), tx_ptr);
                    }
                    Ok(())
                })
                .map(|_| Ok(None)),
        );
        let eager_fetcher: SqlitePollFetcher<Args, CompactType, Decode> =
            SqlitePollFetcher::new(&self.pool, &self.config, &worker);
        let lazy_fetcher = listener
            .filter(|a| ready(a.operation() == INSERT_OPERATION && a.table_name() == JOBS_TABLE))
            .ready_chunks(self.config.buffer_size())
            .then(move |_| fetch_next::<Args, Decode>(pool.clone(), config.clone(), worker.clone()))
            .flat_map(|res| match res {
                Ok(tasks) => stream::iter(tasks).map(Ok).boxed(),
                Err(e) => stream::iter(vec![Err(e)]).boxed(),
            })
            .map(|res| match res {
                Ok(task) => Ok(Some(task)),
                Err(e) => Err(e),
            });

        register_worker
            .chain(select(lazy_fetcher, eager_fetcher))
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis_workflow::{WorkFlow, WorkflowError};
    use chrono::Local;

    use apalis_core::{
        backend::{
            WeakTaskSink,
            poll_strategy::{IntervalStrategy, StrategyBuilder},
        },
        error::BoxDynError,
        task::data::Data,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use serde::{Deserialize, Serialize};

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        const ITEMS: usize = 10;
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        SqliteStorage::setup(&pool).await.unwrap();

        let mut backend = SqliteStorage::new(&pool);

        let mut start = 0;

        let mut items = stream::repeat_with(move || {
            start += 1;
            start
        })
        .take(ITEMS);
        backend.push_stream(&mut items).await.unwrap();

        println!("Starting worker at {}", Local::now());

        async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
            if ITEMS == item {
                wrk.stop().unwrap();
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-1")
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn hooked_worker() {
        const ITEMS: usize = 20;
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        SqliteStorage::setup(&pool).await.unwrap();

        let lazy_strategy = StrategyBuilder::new()
            .apply(IntervalStrategy::new(Duration::from_secs(5)))
            .build();
        let config = Config::new("rango-tango-queue")
            .with_poll_interval(lazy_strategy)
            .set_buffer_size(5);
        let backend = SqliteStorage::new_with_callback(&pool, &config);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut start = 0;

            let items = stream::repeat_with(move || {
                start += 1;

                Task::builder(serde_json::to_vec(&start).unwrap())
                    .run_after(Duration::from_secs(1))
                    .with_ctx(SqlContext::new().with_priority(start))
                    .build()
            })
            .take(ITEMS)
            .collect::<Vec<_>>()
            .await;
            sink::push_tasks(pool, config, items).await.unwrap();
        });

        async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
            // Priority is in reverse order
            if item == 1 {
                apalis_core::timer::sleep(Duration::from_secs(1)).await;
                wrk.stop().unwrap();
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-1")
            .backend(backend)
            .build(send_reminder);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_workflow() {
        let workflow = WorkFlow::new("odd-numbers-workflow")
            .then(|a: usize| async move { Ok::<_, WorkflowError>((0..=a).collect::<Vec<_>>()) })
            .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 3 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 5 != 0 { Some(x) } else { None } })
            .delay_for(Duration::from_millis(1000))
            .then(|a: Vec<usize>| async move {
                println!("Sum: {}", a.iter().sum::<usize>());
                Err::<(), WorkflowError>(WorkflowError::MissingContextError)
            });

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        SqliteStorage::setup(&pool).await.unwrap();
        let mut sqlite = SqliteStorage::new_with_callback(
            &pool,
            &Config::new("workflow-queue").with_poll_interval(
                StrategyBuilder::new()
                    .apply(IntervalStrategy::new(Duration::from_millis(100)))
                    .build(),
            ),
        );

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

    #[tokio::test]
    async fn test_workflow_complete() {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct PipelineConfig {
            min_confidence: f32,
            enable_sentiment: bool,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct UserInput {
            text: String,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Classified {
            text: String,
            label: String,
            confidence: f32,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct Summary {
            text: String,
            sentiment: Option<String>,
        }

        let workflow = WorkFlow::new("text-pipeline")
            // Step 1: Preprocess input (e.g., tokenize, lowercase)
            .then(|input: UserInput, mut worker: WorkerContext| async move {
                worker.emit(&Event::Custom(Box::new(format!(
                    "Preprocessing input: {}",
                    input.text
                ))));
                let processed = input.text.to_lowercase();
                Ok::<_, WorkflowError>(processed)
            })
            // Step 2: Classify text
            .then(|text: String| async move {
                let confidence = 0.85; // pretend model confidence
                let items = text.split_whitespace().collect::<Vec<_>>();
                let results = items
                    .into_iter()
                    .map(|x| Classified {
                        text: x.to_string(),
                        label: if x.contains("rust") {
                            "Tech"
                        } else {
                            "General"
                        }
                        .to_string(),
                        confidence,
                    })
                    .collect::<Vec<_>>();
                Ok::<_, WorkflowError>(results)
            })
            // Step 3: Filter out low-confidence predictions
            .filter_map(
                |c: Classified| async move { if c.confidence >= 0.6 { Some(c) } else { None } },
            )
            .filter_map(move |c: Classified, config: Data<PipelineConfig>| {
                let cfg = config.enable_sentiment;
                async move {
                    if !cfg {
                        return Some(Summary {
                            text: c.text,
                            sentiment: None,
                        });
                    }

                    // pretend we run a sentiment model
                    let sentiment = if c.text.contains("delightful") {
                        "positive"
                    } else {
                        "neutral"
                    };
                    Some(Summary {
                        text: c.text,
                        sentiment: Some(sentiment.to_string()),
                    })
                }
            })
            .then(|a: Vec<Summary>, mut worker: WorkerContext| async move {
                dbg!(&a);
                worker.emit(&Event::Custom(Box::new(format!(
                    "Generated {} summaries",
                    a.len()
                ))));
                worker.stop()
            });

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        SqliteStorage::setup(&pool).await.unwrap();
        let mut sqlite = SqliteStorage::new_with_callback(&pool, &Config::new("text-pipeline"));

        let input = UserInput {
            text: "Rust makes systems programming delightful!".to_string(),
        };
        sqlite.push(input).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(sqlite)
            .data(PipelineConfig {
                min_confidence: 0.8,
                enable_sentiment: true,
            })
            .on_event(|ctx, ev| match ev {
                Event::Custom(msg) => {
                    if let Some(m) = msg.downcast_ref::<String>() {
                        println!("Custom Message: {}", m);
                    }
                }
                Event::Error(_) => {
                    println!("On Error = {:?}", ev);
                    ctx.stop().unwrap();
                }
                _ => {
                    println!("On Event = {:?}", ev);
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
