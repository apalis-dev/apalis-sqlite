use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    ffi::c_void,
    future::ready,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    Config, INSERT_OPERATION, JOBS_TABLE, SqliteStorage, SqliteTask,
    ack::SqliteAck,
    context::SqliteContext,
    fetcher::SqliteFetcher,
    heartbeat,
    hook::{DbEvent, update_hook_callback},
};
use crate::{from_row::TaskRow, sink::SqliteSink};
use apalis_core::{
    backend::{
        Backend, TaskStream,
        codec::{Codec, json::JsonCodec},
        shared::MakeShared,
    },
    task::{Task, task_id::TaskId},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use chrono::Utc;
use futures::{
    FutureExt, SinkExt, Stream, StreamExt, TryStreamExt,
    channel::mpsc::{self, Receiver, Sender},
    future::{self, BoxFuture, Shared},
    lock::Mutex,
    stream::{self, BoxStream, select},
};
use libsqlite3_sys::{sqlite3, sqlite3_update_hook};
use serde_json::Value;
use sqlx::{SqlitePool, pool};
use ulid::Ulid;

pub struct SharedSqliteStorage {
    pool: SqlitePool,
    registry: Arc<Mutex<HashMap<String, Sender<SqliteTask<String>>>>>,
    drive: Shared<BoxFuture<'static, ()>>,
}

impl SharedSqliteStorage {
    pub fn new(pool: SqlitePool) -> Self {
        let registry: Arc<Mutex<HashMap<String, Sender<SqliteTask<String>>>>> =
            Arc::new(Mutex::new(HashMap::default()));
        let p = pool.clone();
        let instances = registry.clone();
        Self {
            pool,
            drive: async move {
                let (tx, rx) = mpsc::unbounded::<DbEvent>();

                let mut conn = p.acquire().await.unwrap();
                // Get raw sqlite3* handle
                let handle: *mut sqlite3 =
                    conn.lock_handle().await.unwrap().as_raw_handle().as_ptr();

                // Put sender in a Box so it has a stable memory address
                let tx_box = Box::new(tx);
                let tx_ptr = Box::into_raw(tx_box) as *mut c_void;

                unsafe {
                    sqlite3_update_hook(handle, Some(update_hook_callback), tx_ptr);
                }

                rx.filter(|a| {
                    ready(a.operation() == INSERT_OPERATION && a.table_name() == JOBS_TABLE)
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
                            TaskRow,
                            "queries/task/fetch_shared.sql",
                            job_types,
                            row_ids,
                            buffer_size,
                        )
                        .fetch(&mut *tx)
                        .map(|r| {
                            Ok::<_, sqlx::Error>(r?.try_into_task::<JsonCodec<String>, String>()?)
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
                                        .namespace()
                                        .as_ref()
                                        .expect("Namespace must be set"),
                                ) {
                                    let _ = tx.send(task).await;
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("Error fetching tasks: {:?}", e);
                        }
                    }
                })
                .await;
            }
            .boxed()
            .shared(),
            registry,
        }
    }
}
#[derive(Debug)]
pub enum SharedPostgresError {
    NamespaceExists(String),
    RegistryLocked,
}

impl<Args> MakeShared<Args> for SharedSqliteStorage {
    type Backend = SqliteStorage<Args, JsonCodec<String>, SharedFetcher>;
    type Config = Config;
    type MakeError = SharedPostgresError;
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
            .ok_or(SharedPostgresError::RegistryLocked)?;
        if let Some(_) = r.insert(config.namespace().to_owned(), tx) {
            return Err(SharedPostgresError::NamespaceExists(
                config.namespace().to_owned(),
            ));
        }
        let sink = SqliteSink::new(&self.pool, &config);
        Ok(SqliteStorage {
            config,
            fetcher: SharedFetcher {
                poller: self.drive.clone(),
                receiver: rx,
            },
            pool: self.pool.clone(),
            sink,
            job_type: PhantomData,
            codec: PhantomData,
        })
    }
}

pub struct SharedFetcher<Compact = String> {
    poller: Shared<BoxFuture<'static, ()>>,
    receiver: Receiver<SqliteTask<Compact>>,
}

impl<Compact> Stream for SharedFetcher<Compact> {
    type Item = SqliteTask<Compact>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Keep the poller alive by polling it, but ignoring the output
        let _ = this.poller.poll_unpin(cx);

        // Delegate actual items to receiver
        this.receiver.poll_next_unpin(cx)
    }
}

impl<Args, Decode> Backend<Args> for SqliteStorage<Args, Decode, SharedFetcher>
where
    Args: Send + 'static + Unpin + Sync,
    Decode: Codec<Args, Compact = String> + 'static + Unpin + Send + Sync,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type IdType = Ulid;

    type Error = sqlx::Error;

    type Stream = TaskStream<SqliteTask<Args>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Codec = Decode;

    type Context = SqliteContext;

    type Layer = AcknowledgeLayer<SqliteAck>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let worker_type = self.config.namespace().to_owned();
        let keep_alive = *self.config.keep_alive();
        let pool = self.pool.clone();
        let worker = worker.clone();

        stream::unfold((), move |()| async move {
            apalis_core::timer::sleep(keep_alive).await;
            Some(((), ()))
        })
        .then(move |_| {
            heartbeat(
                pool.clone(),
                worker_type.clone(),
                worker.clone(),
                Utc::now().timestamp(),
                "SharedSqliteStorage",
            )
        })
        .boxed()
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(SqliteAck::new(self.pool.clone()))
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        // TODO: Update lock_by field in the database to worker.name()
        let _worker_id = worker.name().to_owned();
        let pool = self.pool.clone();
        let worker = worker.clone();
        let worker_type = self.config.namespace().to_owned();
        // Initial registration heartbeat
        // This ensures that the worker is registered before fetching any tasks
        // This also ensures that the worker is marked as alive in case it crashes
        // before fetching any tasks
        // Subsequent heartbeats are handled in the heartbeat stream
        let init = heartbeat(
            pool.clone(),
            worker_type.clone(),
            worker.clone(),
            Utc::now().timestamp(),
            "SharedSqliteStorage",
        );
        let starter = stream::once(init)
            .filter_map(|s| future::ready(s.ok().map(|_| Ok(None::<SqliteTask<Args>>))))
            .boxed();
        let lazy_fetcher = self
            .fetcher
            .map(|t| {
                t.try_map(|args| Decode::decode(&args).map_err(|e| sqlx::Error::Decode(e.into())))
            })
            .flat_map(|vec| match vec {
                Ok(task) => stream::iter(vec![Ok(Some(task))]).boxed(),
                Err(e) => stream::once(ready(Err(e))).boxed(),
            })
            .boxed();

        let eager_fetcher = StreamExt::boxed(SqliteFetcher::<Args, String, Decode>::new(
            &self.pool,
            &self.config,
            &worker,
        ));
        starter.chain(select(lazy_fetcher, eager_fetcher)).boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use chrono::Local;

    use apalis_core::{
        backend::{TaskSink, memory::MemoryStorage},
        error::BoxDynError,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };

    use crate::context::SqliteContext;

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();

        SqliteStorage::setup(&pool).await.unwrap();

        let mut store = SharedSqliteStorage::new(pool);

        let mut map_store = store.make_shared().unwrap();

        let mut int_store = store.make_shared().unwrap();

        let task = Task::builder(99u32)
            .run_after(Duration::from_secs(2))
            .with_ctx(SqliteContext::new().with_priority(1))
            .build();

        map_store
            .send_all(&mut stream::iter(vec![task].into_iter().map(Ok)))
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
