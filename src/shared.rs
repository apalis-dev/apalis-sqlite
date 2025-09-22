use std::{
    collections::{HashMap, HashSet},
    future::ready,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    Config, InsertEvent, SqliteTask, PostgresStorage, ack::SqliteAck, context::SqliteContext,
    fetcher::SqliteFetcher, register,
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
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::{SqlitePool, postgres::SqliteListener};
use ulid::Ulid;

pub struct SharedPostgresStorage<Compact = Value, Codec = JsonCodec<Value>> {
    pool: SqlitePool,
    registry: Arc<Mutex<HashMap<String, Sender<TaskId>>>>,
    drive: Shared<BoxFuture<'static, ()>>,
    _marker: PhantomData<(Compact, Codec)>,
}

impl SharedPostgresStorage {
    pub fn new(pool: SqlitePool) -> Self {
        let registry: Arc<Mutex<HashMap<String, Sender<TaskId>>>> =
            Arc::new(Mutex::new(HashMap::default()));
        let p = pool.clone();
        let instances = registry.clone();
        Self {
            pool,
            drive: async move {
                let mut listener = SqliteListener::connect_with(&p).await.unwrap();
                listener.listen("apalis::job::insert").await.unwrap();
                listener
                    .into_stream()
                    .filter_map(|notification| {
                        let instances = instances.clone();
                        async move {
                            let Sqlite_notification = notification.ok()?;
                            let payload = Sqlite_notification.payload();
                            let ev: InsertEvent = serde_json::from_str(payload).ok()?;
                            let instances = instances.lock().await;
                            if instances.get(&ev.job_type).is_some() {
                                return Some(ev);
                            }
                            None
                        }
                    })
                    .for_each(|ev| {
                        let instances = instances.clone();
                        async move {
                            let mut instances = instances.lock().await;
                            let sender = instances.get_mut(&ev.job_type).unwrap();
                            sender.send(ev.id).await.unwrap();
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
#[derive(Debug)]
pub enum SharedPostgresError {
    NamespaceExists(String),
    RegistryLocked,
}

impl<Args, Compact, Codec> MakeShared<Args> for SharedPostgresStorage<Compact, Codec> {
    type Backend = PostgresStorage<Args, Compact, Codec, SharedFetcher>;
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
        Ok(PostgresStorage {
            _marker: PhantomData,
            config,
            fetcher: SharedFetcher {
                poller: self.drive.clone(),
                receiver: rx,
            },
            pool: self.pool.clone(),
            sink,
        })
    }
}

pub struct SharedFetcher {
    poller: Shared<BoxFuture<'static, ()>>,
    receiver: Receiver<TaskId>,
}

impl Stream for SharedFetcher {
    type Item = TaskId;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Keep the poller alive by polling it, but ignoring the output
        let _ = this.poller.poll_unpin(cx);

        // Delegate actual items to receiver
        this.receiver.poll_next_unpin(cx)
    }
}

impl<Args, Decode> Backend<Args> for PostgresStorage<Args, Value, Decode, SharedFetcher>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = Value> + 'static + Unpin + Send,
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
        let fut = register(
            self.pool.clone(),
            worker_type,
            worker.clone(),
            Utc::now().timestamp(),
            "SharedPostgresStorage",
        );
        stream::once(fut).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(SqliteAck::new(self.pool.clone()))
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let pool = self.pool.clone();
        let worker_id = worker.name().to_owned();
        let lazy_fetcher = self
            .fetcher
            .map(|t| t.to_string())
            .ready_chunks(self.config.buffer_size())
            .then(move |ids| {
                let pool = pool.clone();
                let worker_id = worker_id.clone();
                async move {
                    let mut tx = pool.begin().await?;
                    let res: Vec<_> = sqlx::query_file_as!(
                        TaskRow,
                        "queries/task/lock_by_id.sql",
                        &ids,
                        &worker_id
                    )
                    .fetch(&mut *tx)
                    .map(|r| Ok(Some(r?.try_into_task::<Decode, Args>()?)))
                    .collect()
                    .await;
                    tx.commit().await?;
                    Ok::<_, sqlx::Error>(res)
                }
            })
            .flat_map(|vec| match vec {
                Ok(vec) => stream::iter(vec.into_iter().map(|res| match res {
                    Ok(t) => Ok(t),
                    Err(e) => Err(e),
                }))
                .boxed(),
                Err(e) => stream::once(ready(Err(e))).boxed(),
            })
            .boxed();

        let eager_fetcher = StreamExt::boxed(SqliteFetcher::<Args, Value, Decode>::new(
            &self.pool,
            &self.config,
            worker,
        ));
        select(lazy_fetcher, eager_fetcher).boxed()
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
        let pool = SqlitePool::connect("postgres://postgres:postgres@localhost/apalis_dev")
            .await
            .unwrap();
        let mut store = SharedPostgresStorage::new(pool);

        let mut map_store = store.make_shared().unwrap();

        let mut int_store = store.make_shared().unwrap();

        let task = Task::builder(99u32)
            .run_after(Duration::from_secs(2))
            .with_ctx({
                let mut ctx = SqliteContext::default();
                ctx.set_priority(1);
                ctx
            })
            .build();

        map_store
            .send_all(&mut stream::iter(vec![task].into_iter().map(Ok)))
            .await
            .unwrap();
        int_store.push(99).await.unwrap();

        async fn send_reminder<T, I>(
            _: T,
            task_id: TaskId<I>,
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
        let res = tokio::try_join!(int_worker.run(), map_worker.run()).unwrap();
    }
}
