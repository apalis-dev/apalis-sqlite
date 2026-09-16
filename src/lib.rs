#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]
#![doc = include_str!("../README.md")]

use std::{
    fmt::Debug,
    marker::PhantomData,
    task::{Context, Poll},
};

pub use apalis_codec::json::JsonCodec;

use apalis_core::{
    backend::{
        Backend, BackendConfig, TryNewBackend, WireFormatBackend,
        ext::{
            BackendExt,
            lifecycle::BeforeStart,
            poll_strategy::{PollWith, StreamStrategy},
        },
        finalize::Durable,
        persistence::{Persisted, TaskPersistLayer},
    },
    features_table,
    task::Task,
    worker::context::WorkerContext,
};
use futures::{
    FutureExt,
    channel::mpsc::{self},
};
use serde_json::Value;
use sqlx::{Sqlite, pool::PoolOptions};
pub use sqlx::{
    SqliteConnection, SqlitePool,
    error::Error as SqlxError,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use ulid::Ulid;

pub mod callback;
mod from_row;
mod persist;
pub mod queries;
pub mod shared;
/// Sink module for pushing tasks to sqlite backend
mod sink;

use persist::SqlxPersistence;
const JOBS_TABLE: &str = "Jobs";

/// An alias for [`Task`] specialized for Sqlite
pub type SqliteTask<Args = Vec<u8>> = Task<Args>;

mod error;

mod config;

pub use config::Config;
pub use error::Error;

use crate::callback::{DbEvent, HookCallbackListener, update_hook_callback};
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
        #   SqliteStorage::<u32>::new(&pool)
        # };
    "#,

    Backend => supported("Supports storage and retrieval of tasks", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => supported("Serialization support for arguments", true),
    Workflow => supported("Flexible enough to support workflows", true),
    WebUI => supported("Expose a web interface for monitoring tasks", true),
    FetchById => supported("Allow fetching a task by its ID", false),
    RegisterWorker => supported("Allow registering a worker with the backend", false),
    BackendFactory => supported("Share one connection across multiple workers via [`SqliteStorageFactory`]", false),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    ResumeById => supported("Resume a task by its ID", false),
    ResumeAbandoned => supported("Resume abandoned tasks", false),
    ListWorkers => supported("List all workers registered with the backend", false),
    ListTasks => supported("List all tasks in the backend", false),
}]
///
/// [`SqliteStorageFactory`]: crate::shared::SqliteStorageFactory
#[pin_project::pin_project]
#[derive(Debug)]
pub struct SqliteStorage<Args> {
    #[pin]
    persistence: Persisted<SqlxPersistence>,
    job_type: PhantomData<Args>,
    codec: JsonCodec,
}

impl<T> Clone for SqliteStorage<T> {
    fn clone(&self) -> Self {
        Self {
            persistence: self.persistence.clone(),
            job_type: PhantomData,
            codec: self.codec.clone(),
        }
    }
}

impl SqliteStorage<()> {
    /// Connects to a database returning a pool and listener
    pub fn connect_with_callback(url: &str) -> Result<(SqlitePool, HookCallbackListener), Error> {
        let (tx, rx) = mpsc::unbounded::<DbEvent>();
        let listener = HookCallbackListener::new(rx);
        let pool = PoolOptions::<Sqlite>::new()
            .after_connect(move |conn, _| {
                let mut tx = tx.clone();
                Box::pin(async move {
                    let mut lock_handle = conn.lock_handle().await?;
                    lock_handle.set_update_hook(move |ev| update_hook_callback(ev, &mut tx));
                    Ok(())
                })
            })
            .connect_lazy(url)?;
        Ok((pool, listener))
    }
    /// Perform migrations for storage
    #[cfg(feature = "migrate")]
    pub async fn setup(pool: &SqlitePool) -> Result<(), Error> {
        sqlx::query("PRAGMA journal_mode = 'WAL';")
            .execute(pool)
            .await?;
        sqlx::query("PRAGMA temp_store = MEMORY;")
            .execute(pool)
            .await?;
        sqlx::query("PRAGMA synchronous = OFF;")
            .execute(pool)
            .await?;
        sqlx::query("PRAGMA cache_size = 64000;")
            .execute(pool)
            .await?;
        sqlx::query("PRAGMA journal_size_limit = 67108864;")
            .execute(pool)
            .await?;
        sqlx::query("PRAGMA optimize;").execute(pool).await?;
        Self::migrations()
            .run(pool)
            .await
            .map_err(sqlx::Error::from)?;
        Ok(())
    }

    /// Get sqlite migrations without running them
    #[cfg(feature = "migrate")]
    #[must_use]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("./migrations")
    }
}

impl<T> SqliteStorage<T> {
    /// Create a new SqliteStorage
    #[must_use]
    pub fn new(pool: &SqlitePool) -> SqliteStorage<T> {
        let config = Config::default().queue(std::any::type_name::<T>());
        SqliteStorage {
            job_type: PhantomData,
            codec: JsonCodec::default(),
            persistence: Persisted::new(SqlxPersistence {
                pool: pool.clone(),
                config,
            }),
        }
    }

    /// Create a new SqliteStorage with a custom configuration
    #[must_use]
    pub fn with_config(mut self, config: Config) -> SqliteStorage<T> {
        self.persistence.config = config;
        self
    }

    /// Attach a callback to an instance
    pub fn with_callback(
        self,
        callback: HookCallbackListener,
    ) -> PollWith<Self, StreamStrategy<HookCallbackListener>> {
        self.poll_with_stream(callback)
    }

    /// Get the underlying pool
    pub fn pool(&self) -> &SqlitePool {
        &self.persistence.pool
    }
}

impl<Args> Backend for SqliteStorage<Args> {
    type Task = Task<Vec<u8>>;

    type Error = Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.persistence
            .poll_ready(cx, worker, self.persistence.config.heartbeat_interval)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<SqliteTask, Self::Error>>> {
        self.persistence.poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.persistence.poll_close(cx, worker)
    }
}

impl<Args> BackendConfig for SqliteStorage<Args> {
    type Args = Args;

    type Id = Ulid;

    type Kind = Durable;

    type Config = Config;

    type Layer = TaskPersistLayer<JsonCodec<Value>, Value>;

    fn config(&self) -> &Self::Config {
        &self.persistence.config
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        self.persistence
            .layer(JsonCodec::default(), self.config().batch_size)
            .persist_results(self.config().persist_results)
            .lock_tasks(self.config().lock_tasks)
    }
}

impl<Args> WireFormatBackend for SqliteStorage<Args> {
    type Codec = JsonCodec<Vec<u8>>;

    type Compact = Vec<u8>;
    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<T> TryNewBackend for SqliteStorage<T> {
    type Backend = BeforeStart<Self, Self::Error>;
    fn try_new(config: Self::Config) -> Result<Self::Backend, Self::Error> {
        let pool = SqlitePoolOptions::new()
            .connect_lazy(config.database_url.as_deref().unwrap_or(":memory:"))?;

        Ok(SqliteStorage::new(&pool).before_start(|s| {
            let pool = s.persistence.pool.clone();
            async move {
                SqliteStorage::setup(&pool).await?;
                Ok(())
            }
            .boxed()
        }))
    }
}
#[cfg(test)]
mod tests {
    use apalis::prelude::*;
    use apalis_codec::bincode::BincodeCodec;
    use apalis_core::backend::ext::BackendExt;
    use apalis_workflow::*;
    use futures::{StreamExt, future::ready, stream};
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;
    use std::time::{Duration, Instant};

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        const ITEMS: usize = 3;
        let url = std::env::var("DATABASE_URL").unwrap_or(":memory:".to_owned());
        let pool = SqlitePool::connect(&url).await.unwrap();

        let mut backend = SqliteStorage::new(&pool).before_start(|b| {
            let pool = b.pool().clone();
            async move { SqliteStorage::setup(&pool).await }
        });

        let mut start: usize = 0;

        let mut items = stream::repeat_with(move || {
            start += 1;
            start
        })
        .take(ITEMS);

        backend.push_stream(&mut items).await.unwrap();

        async fn send_reminder(item: usize, wrk: WorkerContext) -> Result<(), BoxDynError> {
            if ITEMS == item {
                wrk.stop().unwrap();
            }
            Ok(())
        }
        let inst = Instant::now();
        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .retry(RetryPolicy::retries(3))
            .build(send_reminder);
        worker.run().await.unwrap();

        println!("Done in {:?}", inst.elapsed());
    }

    #[tokio::test]
    async fn hooked_worker() {
        const ITEMS: usize = 10;
        let url = &std::env::var("DATABASE_URL").unwrap_or(":memory:".to_owned());
        let (pool, callback) = SqliteStorage::connect_with_callback(url).unwrap();
        SqliteStorage::setup(&pool).await.unwrap();

        let backend = SqliteStorage::new(&pool).with_callback(callback);
        let queue = backend.config().queue.to_string();

        tokio::spawn(async move {
            let mut start = 0;
            tokio::time::sleep(Duration::from_secs(5)).await;
            loop {
                start += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;

                let items = stream::repeat_with(move || {
                    TaskBuilder::new(serde_json::to_vec(&start).unwrap()).build()
                })
                .take(1)
                .collect::<Vec<_>>()
                .await;
                let mut tx = pool.begin().await.unwrap();
                let _ = crate::sink::push_tasks(&mut tx, &queue, &items).await;
                tx.commit().await.unwrap();
            }
        });

        async fn send_reminder(
            item: usize,
            wrk: WorkerContext,
            token: TaskContext,
        ) -> Result<(), BoxDynError> {
            let _ctx = token.execution_context().unwrap();
            if item == ITEMS {
                wrk.emit(format!("Processed {} tasks", ITEMS));
                wrk.stop().unwrap();
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-hooked")
            .backend(backend)
            .on_event(|_, ev| println!("{ev}"))
            .build(send_reminder);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_workflow() {
        let workflow = SteppedFlow::new("odd-numbers-workflow")
            .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..=a).collect::<Vec<_>>()) })
            .delay_for(Duration::from_millis(5000))
            .filter_map(|x| ready(if x % 2 != 0 { Some(x) } else { None }))
            .and_then(|a: Vec<usize>| async move {
                println!("Sum: {}", a.iter().sum::<usize>());
                Err::<(), BoxDynError>("Intentional Error".into())
            });

        let pool =
            SqlitePool::connect(&std::env::var("DATABASE_URL").unwrap_or(":memory:".to_owned()))
                .await
                .unwrap();
        SqliteStorage::setup(&pool).await.unwrap();

        let mut sqlite = SqliteStorage::new(&pool)
            .with_codec(BincodeCodec)
            // Our worker may sleep at the delay, (our heartbeat is 30s, so we wake the worker every second)
            .poll_with_interval(Duration::from_secs(1));

        sqlite.push_start(42).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango-workflow")
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

        let workflow = SteppedFlow::new("text-pipeline")
            // Step 1: Preprocess input (e.g., tokenize, lowercase)
            .and_then(|input: UserInput, worker: WorkerContext| async move {
                worker.emit(format!("Preprocessing input: {}", input.text));
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
            .delay_for(Duration::from_millis(5000))
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
            .and_then(|a: Vec<Summary>, worker: WorkerContext| async move {
                worker.emit(format!("Generated {} summaries", a.len()));
                worker.stop()
            });

        let pool = SqlitePool::connect(":memory:").await.unwrap();

        SqliteStorage::setup(&pool).await.unwrap();

        let backoff = BackoffConfig::new(Duration::from_millis(5000));
        let mut sqlite =
            SqliteStorage::new(&pool).poll_with_backoff(Duration::from_millis(200), backoff);

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
