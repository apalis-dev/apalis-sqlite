use std::{fmt, marker::PhantomData};

use apalis_core::{
    backend::{
        Backend, TaskStream,
        codec::{Codec, json::JsonCodec},
    },
    task::Task,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use chrono::{DateTime, Utc};
use futures::{
    StreamExt,
    channel::mpsc,
    future::ready,
    stream::{self, BoxStream, select},
};
use libsqlite3_sys::{sqlite3, sqlite3_update_hook};
use sqlx::{Pool, Sqlite, SqlitePool};
use std::ffi::c_void;
use ulid::Ulid;

use crate::{
    ack::SqliteAck,
    fetcher::{SqliteFetcher, fetch_next},
    hook::update_hook_callback,
    sink::SqliteSink,
};

mod ack;
mod config;
mod context;
mod fetcher;
pub mod from_row;
mod hook;
mod shared;
mod sink;

pub type SqliteTask<Args> = Task<Args, SqliteContext, Ulid>;
pub use config::Config;
pub use context::SqliteContext;
pub use hook::{DbEvent, HookListener};
pub use shared::{SharedPostgresError, SharedSqliteStorage};

type DefaultFetcher<Args> = PhantomData<SqliteFetcher<Args, String, JsonCodec<String>>>;

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
        let config = Config::default();
        Self {
            pool: pool.clone(),
            job_type: PhantomData,
            sink: SqliteSink::new(pool, &config),
            config,
            codec: PhantomData,
            fetcher: PhantomData,
        }
    }

    pub fn new_with_codec<Codec>(pool: &Pool<Sqlite>, config: &Config) -> SqliteStorage<T, Codec> {
        SqliteStorage {
            pool: pool.clone(),
            job_type: PhantomData,
            config: config.clone(),
            codec: PhantomData,
            sink: SqliteSink::new(pool, config),
            fetcher: PhantomData,
        }
    }

    pub fn new_with_config(pool: &Pool<Sqlite>, config: &Config) -> Self {
        Self {
            pool: pool.clone(),
            job_type: PhantomData,
            config: config.clone(),
            codec: PhantomData,
            sink: SqliteSink::new(pool, config),
            fetcher: PhantomData,
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

impl<Args, Decode> Backend<Args> for SqliteStorage<Args, Decode>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = String> + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type IdType = Ulid;

    type Context = SqliteContext;

    type Codec = Decode;

    type Error = sqlx::Error;

    type Stream = SqliteFetcher<Args, String, Decode>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = AcknowledgeLayer<SqliteAck>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let worker_type = self.config.namespace().to_owned();
        let fut = heartbeat(
            self.pool.clone(),
            worker_type,
            worker.clone(),
            Utc::now().timestamp(),
            "SqliteStorage",
        );
        stream::once(fut).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(SqliteAck::new(self.pool.clone()))
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        SqliteFetcher::new(&self.pool, &self.config, worker)
    }
}

impl<Args, Decode> Backend<Args> for SqliteStorage<Args, Decode, HookListener>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = String> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type IdType = Ulid;

    type Context = SqliteContext;

    type Codec = Decode;

    type Error = sqlx::Error;

    type Stream = TaskStream<SqliteTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = AcknowledgeLayer<SqliteAck>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let worker_type = self.config.namespace().to_owned();
        let fut = heartbeat(
            self.pool.clone(),
            worker_type,
            worker.clone(),
            Utc::now().timestamp(),
            "SqliteStorageWithHook",
        );
        stream::once(fut).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(SqliteAck::new(self.pool.clone()))
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let pool = self.pool.clone();
        let config = self.config.clone();
        let worker = worker.clone();
        let eager_fetcher: SqliteFetcher<Args, String, Decode> =
            SqliteFetcher::new(&self.pool, &self.config, &worker);
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

        select(lazy_fetcher, eager_fetcher).boxed()
    }
}

pub(crate) async fn heartbeat(
    pool: SqlitePool,
    worker_type: String,
    worker: WorkerContext,
    last_seen: i64,
    backend_type: &str,
) -> Result<(), sqlx::Error> {
    let last_seen = DateTime::from_timestamp(last_seen, 0).ok_or(sqlx::Error::Io(
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid Timestamp"),
    ))?;
    let svc = worker.get_service().to_owned();
    let worker = worker.name().to_owned();
    let res = sqlx::query_file!(
        "queries/worker/register.sql",
        worker,
        worker_type,
        backend_type,
        svc,
        last_seen
    )
    .execute(&pool)
    .await?;
    if res.rows_affected() == 0 {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
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
        let config = Config::default()
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
