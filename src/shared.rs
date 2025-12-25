use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    future::ready,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    CompactType, Config, JOBS_TABLE, SqliteContext, SqliteStorage, SqliteTask,
    ack::{LockTaskLayer, SqliteAck},
    callback::{DbEvent, update_hook_callback},
    fetcher::SqlitePollFetcher,
    initial_heartbeat, keep_alive,
};
use crate::{from_row::SqliteTaskRow, sink::SqliteSink};
use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, TaskStream, codec::Codec, shared::MakeShared},
    layers::Stack,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use apalis_sql::from_row::TaskRow;

use crate::SqliteDateTime;
use futures::{
    FutureExt, SinkExt, Stream, StreamExt, TryStreamExt,
    channel::mpsc::{self, Receiver, Sender},
    future::{BoxFuture, Shared},
    lock::Mutex,
    stream::{self, BoxStream, select},
};
use sqlx::{Sqlite, SqlitePool, pool::PoolOptions, sqlite::SqliteOperation};
use ulid::Ulid;

/// Shared Sqlite storage backend that can be used across multiple workers
#[derive(Clone, Debug)]
pub struct SharedSqliteStorage<Decode> {
    pool: SqlitePool,
    registry: Arc<Mutex<HashMap<String, Sender<SqliteTask<CompactType>>>>>,
    drive: Shared<BoxFuture<'static, ()>>,
    _marker: PhantomData<Decode>,
}

impl<Decode> SharedSqliteStorage<Decode> {
    /// Get a reference to the underlying Sqlite connection pool
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

impl SharedSqliteStorage<JsonCodec<CompactType>> {
    /// Create a new shared Sqlite storage backend with the given database URL
    #[must_use]
    pub fn new(url: &str) -> Self {
        Self::new_with_codec(url)
    }
    /// Create a new shared Sqlite storage backend with the given database URL and codec
    #[must_use]
    pub fn new_with_codec<Codec>(url: &str) -> SharedSqliteStorage<Codec> {
        let (tx, rx) = mpsc::unbounded::<DbEvent>();
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

        let registry: Arc<Mutex<HashMap<String, Sender<SqliteTask<CompactType>>>>> =
            Arc::new(Mutex::new(HashMap::default()));
        let p = pool.clone();
        let instances = registry.clone();
        SharedSqliteStorage {
            pool,
            drive: async move {
                rx.filter(|a| {
                    ready(a.operation() == &SqliteOperation::Insert && a.table_name() == JOBS_TABLE)
                })
                .ready_chunks(instances.try_lock().map(|r| r.len()).unwrap_or(10))
                .then(|events| {
                    let row_ids = events.iter().map(|e| e.rowid()).collect::<HashSet<i64>>();
                    let instances = instances.clone();
                    let pool = p.clone();
                    async move {
                        let instances = instances.lock().await;
                        let job_types = serde_json::to_string(
                            &instances.keys().cloned().collect::<Vec<String>>(),
                        )
                        .unwrap();
                        let row_ids = serde_json::to_string(&row_ids).unwrap();
                        let mut tx = pool.begin().await?;
                        let buffer_size = max(10, instances.len()) as i32;
                        let res: Vec<_> = sqlx::query_file_as!(
                            SqliteTaskRow,
                            "queries/backend/fetch_next_shared.sql",
                            job_types,
                            row_ids,
                            buffer_size,
                        )
                        .fetch(&mut *tx)
                        .map(|r| {
                            let row: TaskRow<SqliteDateTime> = r?.try_into()?;
                            row.try_into_task_compact()
                                .map_err(|e| sqlx::Error::Protocol(e.to_string()))
                        })
                        .try_collect()
                        .await?;
                        tx.commit().await?;
                        Ok::<_, sqlx::Error>(res)
                    }
                })
                .for_each(|r| async {
                    match r {
                        Ok(tasks) => {
                            let mut instances = instances.lock().await;
                            for task in tasks {
                                if let Some(tx) = instances.get_mut(
                                    task.parts
                                        .ctx
                                        .queue()
                                        .as_ref()
                                        .expect("Namespace must be set"),
                                ) {
                                    let _ = tx.send(task).await;
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("Error fetching tasks: {e:?}");
                        }
                    }
                })
                .await;
            }
            .boxed()
            .shared(),
            registry,
            _marker: PhantomData,
        }
    }
}

/// Errors that can occur when creating a shared Sqlite storage backend
#[derive(Debug, thiserror::Error)]
pub enum SharedSqliteError {
    /// Namespace already exists in the registry
    #[error("Namespace {0} already exists")]
    NamespaceExists(String),
    /// Could not acquire registry loc
    #[error("Could not acquire registry lock")]
    RegistryLocked,
}

impl<Args, Decode: Codec<Args, Compact = CompactType>> MakeShared<Args>
    for SharedSqliteStorage<Decode>
{
    type Backend = SqliteStorage<Args, Decode, SharedFetcher<CompactType>>;
    type Config = Config;
    type MakeError = SharedSqliteError;
    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>
    where
        Self::Config: Default,
    {
        Self::make_shared_with_config(self, Config::new(std::any::type_name::<Args>()))
    }
    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (tx, rx) = mpsc::channel(config.buffer_size());
        let mut r = self
            .registry
            .try_lock()
            .ok_or(SharedSqliteError::RegistryLocked)?;
        if r.insert(config.queue().to_string(), tx).is_some() {
            return Err(SharedSqliteError::NamespaceExists(
                config.queue().to_string(),
            ));
        }
        let sink = SqliteSink::new(&self.pool, &config);
        Ok(SqliteStorage {
            config,
            fetcher: SharedFetcher {
                poller: self.drive.clone(),
                receiver: Arc::new(std::sync::Mutex::new(rx)),
            },
            pool: self.pool.clone(),
            sink,
            job_type: PhantomData,
            codec: PhantomData,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SharedFetcher<Compact> {
    poller: Shared<BoxFuture<'static, ()>>,
    receiver: Arc<std::sync::Mutex<Receiver<SqliteTask<Compact>>>>,
}

impl<Compact> Stream for SharedFetcher<Compact> {
    type Item = SqliteTask<Compact>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Keep the poller alive by polling it, but ignoring the output
        let _ = this.poller.poll_unpin(cx);

        // Delegate actual items to receiver
        this.receiver.lock().unwrap().poll_next_unpin(cx)
    }
}

impl<Args, Decode> Backend for SqliteStorage<Args, Decode, SharedFetcher<CompactType>>
where
    Args: Send + 'static + Unpin + Sync,
    Decode: Codec<Args, Compact = CompactType> + 'static + Unpin + Send + Sync,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;

    type IdType = Ulid;

    type Error = sqlx::Error;

    type Stream = TaskStream<SqliteTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Context = SqliteContext;

    type Layer = Stack<AcknowledgeLayer<SqliteAck>, LockTaskLayer>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let keep_alive_interval = *self.config.keep_alive();
        let pool = self.pool.clone();
        let worker = worker.clone();
        let config = self.config.clone();

        stream::unfold((), move |()| async move {
            apalis_core::timer::sleep(keep_alive_interval).await;
            Some(((), ()))
        })
        .then(move |_| keep_alive(pool.clone(), config.clone(), worker.clone()))
        .boxed()
    }

    fn middleware(&self) -> Self::Layer {
        let lock = LockTaskLayer::new(self.pool.clone());
        let ack = AcknowledgeLayer::new(SqliteAck::new(self.pool.clone()));
        Stack::new(ack, lock)
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.poll_shared(worker)
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

impl<Args, Decode: Send + 'static> BackendExt
    for SqliteStorage<Args, Decode, SharedFetcher<CompactType>>
where
    Self: Backend<Args = Args, IdType = Ulid, Context = SqliteContext, Error = sqlx::Error>,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + 'static + Unpin,
{
    type Codec = Decode;
    type Compact = CompactType;
    type CompactStream = TaskStream<SqliteTask<Self::Compact>, sqlx::Error>;

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_shared(worker).boxed()
    }
}

impl<Args, Decode: Send + 'static> SqliteStorage<Args, Decode, SharedFetcher<CompactType>> {
    fn poll_shared(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<SqliteTask<CompactType>>, sqlx::Error>> + 'static {
        let pool = self.pool.clone();
        let worker = worker.clone();
        // Initial registration heartbeat
        // This ensures that the worker is registered before fetching any tasks
        // This also ensures that the worker is marked as alive in case it crashes
        // before fetching any tasks
        // Subsequent heartbeats are handled in the heartbeat stream
        let init = initial_heartbeat(
            pool,
            self.config.clone(),
            worker.clone(),
            "SharedSqliteStorage",
        );
        let starter = stream::once(init)
            .map_ok(|_| None) // Noop after initial heartbeat
            .boxed();
        let lazy_fetcher = self.fetcher.map(|s| Ok(Some(s))).boxed();

        let eager_fetcher = StreamExt::boxed(SqlitePollFetcher::<CompactType, Decode>::new(
            &self.pool,
            &self.config,
            &worker,
        ));
        starter.chain(select(lazy_fetcher, eager_fetcher)).boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis_core::{
        backend::TaskSink, error::BoxDynError, task::task_id::TaskId,
        worker::builder::WorkerBuilder,
    };

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        let mut store = SharedSqliteStorage::new(":memory:");
        SqliteStorage::setup(store.pool()).await.unwrap();

        let mut map_store = store.make_shared().unwrap();

        let mut int_store = store.make_shared().unwrap();

        map_store
            .push(HashMap::<String, i32>::from([("value".to_string(), 42)]))
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
}
