#![doc = include_str!("../README.md")]
//!
//! [`SqliteStorageWithHook`]: crate::SqliteStorage
use std::{fmt, marker::PhantomData};

use apalis_core::{
    backend::{
        Backend, BackendExt, TaskStream,
        codec::{Codec, json::JsonCodec},
    },
    features_table,
    layers::Stack,
    task::Task,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
pub use apalis_sql::context::SqlContext;
use futures::{
    FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt,
    channel::mpsc::{self},
    future::ready,
    stream::{self, BoxStream, select},
};
pub use sqlx::{
    Connection, Pool, Sqlite, SqliteConnection, SqlitePool,
    pool::{PoolConnection, PoolOptions},
    sqlite::{SqliteConnectOptions, SqliteOperation},
};
use ulid::Ulid;

use crate::{
    ack::{LockTaskLayer, SqliteAck},
    callback::update_hook_callback,
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
/// Fetcher module for retrieving tasks from sqlite backend
pub mod fetcher;
mod from_row;
/// Queries module for sqlite backend
pub mod queries;
mod shared;
/// Sink module for pushing tasks to sqlite backend
pub mod sink;

/// Type alias for a task stored in sqlite backend
pub type SqliteTask<Args> = Task<Args, SqlContext, Ulid>;
pub use callback::{DbEvent, HookCallbackListener};
pub use config::Config;
pub use shared::{SharedSqliteError, SharedSqliteStorage};

/// CompactType is the type used for compact serialization in sqlite backend
pub type CompactType = Vec<u8>;

const JOBS_TABLE: &str = "Jobs";

/// SqliteStorage is a storage backend for apalis using sqlite as the database.
///
/// It supports both standard polling and event-driven (hooked) storage mechanisms.
///
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_sqlite::SqliteStorage;
        #   use sqlx::SqlitePool;
        #   let pool = SqlitePool::connect(":memory:").await.unwrap();
        #   SqliteStorage::setup(&pool).await.unwrap();
        #   SqliteStorage::new(&pool)
        # };
    "#,

    Backend => supported("Supports storage and retrieval of tasks", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => supported("Serialization support for arguments", true),
    Workflow => supported("Flexible enough to support workflows", true),
    WebUI => supported("Expose a web interface for monitoring tasks", true),
    FetchById => supported("Allow fetching a task by its ID", false),
    RegisterWorker => supported("Allow registering a worker with the backend", false),
    MakeShared => supported("Share one connection across multiple workers via [`SharedSqliteStorage`]", false),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    ResumeById => supported("Resume a task by its ID", false),
    ResumeAbandoned => supported("Resume abandoned tasks", false),
    ListWorkers => supported("List all workers registered with the backend", false),
    ListTasks => supported("List all tasks in the backend", false),
}]
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
        Self {
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
    #[must_use]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("./migrations")
    }
}

impl<T> SqliteStorage<T, (), ()> {
    /// Create a new SqliteStorage
    #[must_use]
    pub fn new(
        pool: &Pool<Sqlite>,
    ) -> SqliteStorage<T, JsonCodec<CompactType>, fetcher::SqliteFetcher> {
        let config = Config::new(std::any::type_name::<T>());
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            sink: SqliteSink::new(pool, &config),
            config,
            codec: PhantomData,
            fetcher: fetcher::SqliteFetcher,
        }
    }

    /// Create a new SqliteStorage for a specific queue
    #[must_use]
    pub fn new_in_queue(
        pool: &Pool<Sqlite>,
        queue: &str,
    ) -> SqliteStorage<T, JsonCodec<CompactType>, fetcher::SqliteFetcher> {
        let config = Config::new(queue);
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            sink: SqliteSink::new(pool, &config),
            config,
            codec: PhantomData,
            fetcher: fetcher::SqliteFetcher,
        }
    }

    /// Create a new SqliteStorage with config
    #[must_use]
    pub fn new_with_config(
        pool: &Pool<Sqlite>,
        config: &Config,
    ) -> SqliteStorage<T, JsonCodec<CompactType>, fetcher::SqliteFetcher> {
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            config: config.clone(),
            codec: PhantomData,
            sink: SqliteSink::new(pool, config),
            fetcher: fetcher::SqliteFetcher,
        }
    }

    /// Create a new SqliteStorage with hook callback listener
    #[must_use]
    pub fn new_with_callback(
        url: &str,
        config: &Config,
    ) -> SqliteStorage<T, JsonCodec<CompactType>, HookCallbackListener> {
        let (tx, rx) = mpsc::unbounded::<DbEvent>();

        let listener = HookCallbackListener::new(rx);
        let pool = PoolOptions::<Sqlite>::new()
            .after_connect(move |conn, _meta| {
                let mut tx = tx.clone();
                Box::pin(async move {
                    let mut lock_handle = conn.lock_handle().await?;
                    lock_handle.set_update_hook(move |ev| update_hook_callback(ev, &mut tx));
                    Ok(())
                })
            })
            .connect_lazy(url)
            .expect("Failed to create Sqlite pool");
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            config: config.clone(),
            codec: PhantomData,
            sink: SqliteSink::new(&pool, config),
            fetcher: listener,
        }
    }
}

impl<T, C, F> SqliteStorage<T, C, F> {
    /// Change the codec used for serialization/deserialization
    pub fn with_codec<D>(self) -> SqliteStorage<T, D, F> {
        SqliteStorage {
            sink: SqliteSink::new(&self.pool, &self.config),
            pool: self.pool,
            job_type: PhantomData,
            config: self.config,
            codec: PhantomData,
            fetcher: self.fetcher,
        }
    }

    /// Get the config used by the storage
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the connection pool used by the storage
    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }
}

impl<Args, Decode> Backend for SqliteStorage<Args, Decode, SqliteFetcher>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + 'static + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;

    type Context = SqlContext;

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
        self.poll_default(worker)
            .map(|a| match a {
                Ok(Some(task)) => Ok(Some(
                    task.try_map(|t| Decode::decode(&t))
                        .map_err(|e| sqlx::Error::Decode(e.into()))?,
                )),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

impl<Args, Decode: Send + 'static> BackendExt for SqliteStorage<Args, Decode, SqliteFetcher>
where
    Self: Backend<Args = Args, IdType = Ulid, Context = SqlContext, Error = sqlx::Error>,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + 'static + Unpin,
{
    type Codec = Decode;
    type Compact = CompactType;
    type CompactStream = TaskStream<SqliteTask<Self::Compact>, sqlx::Error>;

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_default(worker).boxed()
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
        self.poll_with_listener(worker)
            .map(|a| match a {
                Ok(Some(task)) => Ok(Some(
                    task.try_map(|t| Decode::decode(&t))
                        .map_err(|e| sqlx::Error::Decode(e.into()))?,
                )),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

impl<Args, Decode: Send + 'static> BackendExt for SqliteStorage<Args, Decode, HookCallbackListener>
where
    Self: Backend<Args = Args, IdType = Ulid, Context = SqlContext, Error = sqlx::Error>,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + 'static + Unpin,
{
    type Codec = Decode;
    type Compact = CompactType;
    type CompactStream = TaskStream<SqliteTask<Self::Compact>, sqlx::Error>;

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_with_listener(worker).boxed()
    }
}

impl<Args, Decode: Send + 'static> SqliteStorage<Args, Decode, HookCallbackListener> {
    fn poll_with_listener(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<SqliteTask<CompactType>>, sqlx::Error>> + Send + 'static
    {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let register_worker = initial_heartbeat(
            self.pool.clone(),
            self.config.clone(),
            worker.clone(),
            "SqliteStorageWithHook",
        );
        let register_worker = stream::once(register_worker.map_ok(|_| None));
        let eager_fetcher: SqlitePollFetcher<CompactType, Decode> =
            SqlitePollFetcher::new(&self.pool, &self.config, &worker);
        let lazy_fetcher = self
            .fetcher
            .filter(|a| {
                ready(a.operation() == &SqliteOperation::Insert && a.table_name() == JOBS_TABLE)
            })
            .inspect(|db_event| {
                log::debug!("Received new job event: {db_event:?}");
            })
            .ready_chunks(self.config.buffer_size())
            .then(move |_| fetch_next(pool.clone(), config.clone(), worker.clone()))
            .flat_map(|res| match res {
                Ok(tasks) => stream::iter(tasks).map(Ok).boxed(),
                Err(e) => stream::iter(vec![Err(e)]).boxed(),
            })
            .map(|res| match res {
                Ok(task) => Ok(Some(task)),
                Err(e) => Err(e),
            });

        register_worker.chain(select(lazy_fetcher, eager_fetcher))
    }
}

impl<Args, Decode: Send + 'static, F> SqliteStorage<Args, Decode, F> {
    fn poll_default(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<SqliteTask<CompactType>>, sqlx::Error>> + Send + 'static
    {
        let fut = initial_heartbeat(
            self.pool.clone(),
            self.config().clone(),
            worker.clone(),
            "SqliteStorage",
        );
        let register = stream::once(fut.map(|_| Ok(None)));
        register.chain(SqlitePollFetcher::<CompactType, Decode>::new(
            &self.pool,
            &self.config,
            worker,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis::prelude::*;
    use apalis_workflow::*;
    use chrono::Local;
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;

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

        let lazy_strategy = StrategyBuilder::new()
            .apply(IntervalStrategy::new(Duration::from_secs(5)))
            .build();
        let config = Config::new("rango-tango-queue")
            .with_poll_interval(lazy_strategy)
            .set_buffer_size(5);
        let backend = SqliteStorage::new_with_callback(":memory:", &config);
        let pool = backend.pool().clone();
        SqliteStorage::setup(&pool).await.unwrap();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut start = 0;

            let items = stream::repeat_with(move || {
                start += 1;

                Task::builder(serde_json::to_vec(&start).unwrap())
                    .with_ctx(SqlContext::new().with_priority(start))
                    .build()
            })
            .take(ITEMS)
            .collect::<Vec<_>>()
            .await;
            crate::sink::push_tasks(pool.clone(), config, items)
                .await
                .unwrap();
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
        let workflow = Workflow::new("odd-numbers-workflow")
            .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..=a).collect::<Vec<_>>()) })
            .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 3 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 5 != 0 { Some(x) } else { None } })
            .delay_for(Duration::from_millis(1000))
            .and_then(|a: Vec<usize>| async move {
                println!("Sum: {}", a.iter().sum::<usize>());
                Err::<(), BoxDynError>("Intentional Error".into())
            });

        let mut sqlite = SqliteStorage::new_with_callback(
            ":memory:",
            &Config::new("workflow-queue").with_poll_interval(
                StrategyBuilder::new()
                    .apply(IntervalStrategy::new(Duration::from_millis(100)))
                    .build(),
            ),
        );

        SqliteStorage::setup(sqlite.pool()).await.unwrap();

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

        let workflow = Workflow::new("text-pipeline")
            // Step 1: Preprocess input (e.g., tokenize, lowercase)
            .and_then(|input: UserInput, mut worker: WorkerContext| async move {
                worker.emit(&Event::custom(format!(
                    "Preprocessing input: {}",
                    input.text
                )));
                let processed = input.text.to_lowercase();
                Ok::<_, BoxDynError>(processed)
            })
            // Step 2: Classify text
            .and_then(|text: String| async move {
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
                Ok::<_, BoxDynError>(results)
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
            .and_then(|a: Vec<Summary>, mut worker: WorkerContext| async move {
                worker.emit(&Event::Custom(Box::new(format!(
                    "Generated {} summaries",
                    a.len()
                ))));
                worker.stop()
            });

        let mut sqlite =
            SqliteStorage::new_with_callback(":memory:", &Config::new("text-pipeline"));

        SqliteStorage::setup(sqlite.pool()).await.unwrap();

        let input = UserInput {
            text: "Rust makes systems programming delightful!".to_string(),
        };
        sqlite.push_start(input).await.unwrap();

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
