use std::{fmt, marker::PhantomData};

use apalis_core::{
    backend::{
        Backend, TaskStream,
        codec::{Codec, json::JsonCodec},
    },
    task::Task,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use futures::{
    FutureExt, StreamExt, TryStreamExt,
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
    fetcher::{SqliteFetcher, SqlitePollFetcher, fetch_next},
    hook::update_hook_callback,
    reenqueue_orphaned::{reenqueue_orphaned, reenqueue_orphaned_stream},
    sink::SqliteSink,
};

mod ack;
mod config;
mod context;
mod fetcher;
pub mod from_row;
mod hook;
mod reenqueue_orphaned;
mod shared;
mod sink;
mod traits;

pub type SqliteTask<Args> = Task<Args, SqliteContext, Ulid>;
pub use config::Config;
pub use context::SqliteContext;
pub use hook::{DbEvent, HookListener};
pub use shared::{SharedPostgresError, SharedSqliteStorage};
pub use sqlx::SqlitePool;

type DefaultFetcher<Args> = SqliteFetcher<Args, String, JsonCodec<String>>;

const INSERT_OPERATION: &str = "INSERT";
const JOBS_TABLE: &str = "Jobs";

#[pin_project::pin_project]
pub struct SqliteStorage<T, C = JsonCodec<String>, Fetcher = DefaultFetcher<T>> {
    pool: Pool<Sqlite>,
    job_type: PhantomData<T>,
    config: Config,
    codec: PhantomData<C>,
    #[pin]
    sink: SqliteSink<T, String, C>,
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

impl SqliteStorage<()> {
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

impl<T> SqliteStorage<T> {
    /// Create a new SqliteStorage
    pub fn new(pool: &Pool<Sqlite>) -> Self {
        let config = Config::new(std::any::type_name::<T>());
        Self {
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

    pub fn new_with_codec<Codec>(pool: &Pool<Sqlite>, config: &Config) -> SqliteStorage<T, Codec> {
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

    pub fn new_with_config(pool: &Pool<Sqlite>, config: &Config) -> Self {
        Self {
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

    pub async fn new_with_hook(
        pool: &Pool<Sqlite>,
        config: &Config,
    ) -> SqliteStorage<T, JsonCodec<String>, HookListener> {
        let pool = pool.clone();
        let (tx, rx) = mpsc::unbounded::<DbEvent>();

        let listener = HookListener::new(rx);

        let mut conn = pool.acquire().await.unwrap();
        // Get raw sqlite3* handle
        let handle: *mut sqlite3 = conn.lock_handle().await.unwrap().as_raw_handle().as_ptr();

        // Put sender in a Box so it has a stable memory address
        let tx_box = Box::new(tx);
        let tx_ptr = Box::into_raw(tx_box) as *mut c_void;

        unsafe {
            sqlite3_update_hook(handle, Some(update_hook_callback), tx_ptr);
        }
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
    pub fn config(&self) -> &Config {
        &self.config
    }
}

impl<Args, Decode> Backend for SqliteStorage<Args, Decode>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = String> + 'static + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;

    type Context = SqliteContext;

    type Codec = Decode;

    type Compact = String;

    type Error = sqlx::Error;

    type Stream = TaskStream<SqliteTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<SqliteAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let keep_alive = stream::unfold((), move |_| {
            let register = keep_alive(pool.clone(), config.clone(), worker.clone());
            let interval = apalis_core::timer::Delay::new(*config.keep_alive());
            interval.then(move |_| register.map(|res| Some((res, ()))))
        });
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
        let worker_type = self.config.queue().to_owned();
        let fut = initial_heartbeat(
            self.pool.clone(),
            self.config().clone(),
            worker.clone(),
            "SqliteStorage",
        );
        let register = stream::once(fut.map(|_| Ok(None)));
        register
            .chain(SqlitePollFetcher::<Args, String, Decode>::new(
                &self.pool,
                &self.config,
                worker,
            ))
            .boxed()
    }
}

impl<Args, Decode> Backend for SqliteStorage<Args, Decode, HookListener>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = String> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;

    type Context = SqliteContext;

    type Codec = Decode;

    type Compact = String;

    type Error = sqlx::Error;

    type Stream = TaskStream<SqliteTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<SqliteAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let worker_type = self.config.queue().to_owned();
        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let keep_alive = stream::unfold((), move |_| {
            let register = keep_alive(pool.clone(), config.clone(), worker.clone());
            let interval = apalis_core::timer::Delay::new(*config.keep_alive());
            interval.then(move |_| register.map(|res| Some((res, ()))))
        });
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
        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let register_worker = initial_heartbeat(
            self.pool.clone(),
            self.config.clone(),
            worker.clone(),
            "SqliteStorageWithHook",
        );
        let register_worker = stream::once(register_worker.map(|_| Ok(None)));
        let eager_fetcher: SqlitePollFetcher<Args, String, Decode> =
            SqlitePollFetcher::new(&self.pool, &self.config, &worker);
        let lazy_fetcher = self
            .fetcher
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

pub async fn initial_heartbeat(
    pool: SqlitePool,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), sqlx::Error> {
    reenqueue_orphaned(pool.clone(), config.clone()).await?;
    register_worker(pool, config, worker, storage_type).await?;
    Ok(())
}

pub(crate) async fn keep_alive(
    pool: SqlitePool,
    config: Config,
    worker: WorkerContext,
) -> Result<(), sqlx::Error> {
    let worker = worker.name().to_owned();
    let queue = config.queue().to_string();
    let res = sqlx::query_file!("queries/backend/keep_alive.sql", worker, queue)
        .execute(&pool)
        .await?;
    if res.rows_affected() == 1 {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "WORKER_DOES_NOT_EXIST",
        )));
    }
    Ok(())
}

pub(crate) async fn register_worker(
    pool: SqlitePool,
    config: Config,
    worker: WorkerContext,
    storage_type: &str,
) -> Result<(), sqlx::Error> {
    let worker_id = worker.name().to_owned();
    let queue = config.queue().to_string();
    let layers = worker.get_service().to_owned();
    let keep_alive = config.keep_alive().as_secs() as i64;
    let res = sqlx::query_file!(
        "queries/backend/register_worker.sql",
        worker_id,
        queue,
        storage_type,
        layers,
        keep_alive,
    )
    .execute(&pool)
    .await?;
    if res.rows_affected() == 0 {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "WORKER_ALREADY_EXISTS",
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Local;

    use apalis_core::{
        backend::poll_strategy::{IntervalStrategy, StrategyBuilder},
        error::BoxDynError,
        worker::builder::WorkerBuilder,
    };
    use futures::SinkExt;

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
            let task = Task::builder(start)
                .run_after(Duration::from_secs(1))
                .with_ctx(SqliteContext::new().with_priority(1))
                .build();
            Ok(task)
        })
        .take(ITEMS);
        backend.send_all(&mut items).await.unwrap();

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
        let backend = SqliteStorage::new_with_hook(&pool, &config).await;

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut start = 0;

            let items = stream::repeat_with(move || {
                start += 1;

                Task::builder(serde_json::to_string(&start).unwrap())
                    .run_after(Duration::from_secs(1))
                    .with_ctx(SqliteContext::new().with_priority(start))
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
}
