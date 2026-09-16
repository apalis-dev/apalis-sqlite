//! Shared SQLite storage for multiple workers.
//!
//! This module provides [`SqliteStorageFactory`], a factory for creating
//! multiple [`SqliteStorage`] instances that share a single SQLite connection
//! pool and task-fetching loop.
//!
//! ## Why shared storage?
//!
//! A SQLite backend can be used by multiple workers, but having every worker
//! independently poll the database can result in unnecessary database queries
//! and contention.
//!
//! [`SqliteStorageFactory`] addresses this by maintaining a
//! single shared polling task for all storage instances created by the factory.
//!
//! When a task is inserted into the jobs table, SQLite's update hook notifies
//! the shared poller.
//!
//! The poller fetches available tasks for all registered
//! queues in a batch and routes each task to the [`SharedFetcher`] belonging to
//! the corresponding storage instance.
//!
//! The resulting architecture is roughly:
//!
//! ```text
//!                         SQLite
//!                            │
//!                     update hook
//!                            │
//!                            ▼
//!                  ┌───────────────────┐
//!                  │  Shared poller    │
//!                  │                   │
//!                  │ fetches tasks for │
//!                  │ all registered    │
//!                  │ queues in batches │
//!                  └─────────┬─────────┘
//!                            │
//!              ┌─────────────┼─────────────┐
//!              │             │             │
//!              ▼             ▼             ▼
//!          queue A       queue B       queue C
//!              │             │             │
//!              ▼             ▼             ▼
//!          Worker A      Worker B      Worker C
//! ```
//!
//! ## Creating a factory
//!
//! The factory owns the underlying [`SqlitePool`] and can create multiple
//! backends from it:
//!
//! ```ignore
//! # use apalis_sqlite::shared::SqliteStorageFactory;
//! # use apalis_core::backend::factory::BackendFactory;
//! let mut factory = SqliteStorageFactory::new("sqlite://jobs.db");
//!
//! let first = factory.create().unwrap();
//! let second = factory.create().unwrap();
//! ```
//!
//! Each backend has its own queue registration and receiver, while the
//! database connection pool and polling task are shared.
//!
//! ## Queue registration
//!
//! Each backend created by the factory is associated with a queue derived from
//! its [`Config`].
//!
//! A queue may only be registered once with a factory.
//!
//! Attempting to create another backend for an already-registered queue returns
//! [`SharedSqliteError::NamespaceExists`].
//!
//! ## Task dispatch
//!
//! [`SharedFetcher`] implements [`Stream`] and receives tasks from the shared
//! polling loop through an asynchronous channel.
//!
//!  The shared poller keeps running while at least one fetcher is being polled and dispatches each
//! fetched task to the channel associated with its queue.
//!
//! The resulting backend is an [`Interleave`] combining the normal
//! [`SqliteStorage`] implementation with [`SharedFetcher`].
//!
//!  This allows task insertion and other storage operations to continue using the normal SQLite
//! backend while task consumption is coordinated by the shared polling loop.
//!
//! ## Pool configuration
//!
//! [`SqliteStorageFactory::new`] creates a pool with unlimited connection
//! lifetime and idle timeout. For applications requiring more control over the
//! SQLite pool, [`SqliteStorageFactory::new_with_pool_options`] accepts custom
//! [`PoolOptions`].
//!
//! [`SqlitePool`]: sqlx::SqlitePool
//! [`PoolOptions`]: sqlx::pool::PoolOptions
//! [`Config`]: crate::Config
//! [`Interleave`]: apalis_core::backend::ext::interleave::Interleave
//! [`SqliteStorage`]: crate::SqliteStorage
//! [`SqliteStorageFactory`]: crate::shared::SqliteStorageFactory
//! [`SharedFetcher`]: crate::shared::SharedFetcher
//! [`SharedSqliteError`]: crate::shared::SharedSqliteError
//! [`Stream`]: futures::Stream
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    future::ready,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    Config, JOBS_TABLE, SqliteStorage, SqliteTask,
    callback::{DbEvent, update_hook_callback},
};
use crate::{Error, from_row::SqliteTaskRow};

use apalis_core::backend::{BackendConfig, ext::interleave::Interleave, factory::BackendFactory};
use futures::{
    FutureExt, SinkExt, Stream, StreamExt, TryStreamExt,
    channel::mpsc::{self, Receiver, Sender},
    future::{BoxFuture, Shared},
    lock::Mutex,
    ready,
};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Sqlite, SqlitePool, pool::PoolOptions, sqlite::SqliteOperation};

/// An [`SqliteStorage`] interleaving with [`SharedFetcher`]
pub type SharedSqliteStorage<Args> = Interleave<SqliteStorage<Args>, SharedFetcher>;

type Registry = Arc<Mutex<HashMap<String, Sender<Result<SqliteTask, Error>>>>>;

/// Shared Sqlite storage backend that can be used across multiple workers
#[derive(Clone, Debug)]
pub struct SqliteStorageFactory {
    pool: SqlitePool,
    registry: Registry,
    drive: Shared<BoxFuture<'static, ()>>,
}

impl SqliteStorageFactory {
    /// Get a reference to the underlying Sqlite connection pool
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

impl SqliteStorageFactory {
    /// Create a new shared Sqlite storage backend with the given database URL and codec
    #[must_use]
    pub fn new(url: &str) -> SqliteStorageFactory {
        Self::new_with_pool_options(
            url,
            PoolOptions::new().max_lifetime(None).idle_timeout(None),
        )
    }

    /// Create a new shared Sqlite storage backend with the given database URL and pool options
    #[must_use]
    pub fn new_with_pool_options(url: &str, options: PoolOptions<Sqlite>) -> SqliteStorageFactory {
        let (tx, rx) = mpsc::unbounded::<DbEvent>();
        let pool = options
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

        let registry: Registry = Registry::default();

        let p = pool.clone();
        let instances = registry.clone();
        SqliteStorageFactory {
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
                        .map_err(Error::JsonError)?;
                        let row_ids = serde_json::to_string(&row_ids).map_err(Error::JsonError)?;
                        let mut tx = pool.begin().await?;
                        let batch_size = max(10, instances.len()) as i32;
                        let res: Vec<_> = sqlx::query_file_as!(
                            SqliteTaskRow,
                            "queries/backend/fetch_next_shared.sql",
                            job_types,
                            row_ids,
                            batch_size,
                        )
                        .fetch(&mut *tx)
                        .map_ok(|r| r.try_into())
                        .try_collect()
                        .await?;
                        tx.commit().await?;
                        Ok::<_, Error>(res)
                    }
                })
                .map_ok(futures::stream::iter)
                .try_flatten()
                .for_each(|r: Result<SqliteTask, Error>| async {
                    match r {
                        Ok(task) => {
                            let mut instances = instances.lock().await;
                            if let Some(tx) = instances
                                .get_mut(&task.queue().expect("Queue must be set").to_string())
                                && let Err(e) = tx.send(Ok(task)).await
                            {
                                log::error!("Error pushing task: {e:?}");
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

impl<Args> BackendFactory<Args> for SqliteStorageFactory
where
    Args: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Backend = SharedSqliteStorage<Args>;
    type Error = SharedSqliteError;

    fn create(&mut self) -> Result<Self::Backend, Self::Error>
    where
        <Self::Backend as BackendConfig>::Config: Default,
    {
        let config = Config::default().queue(std::any::type_name::<Args>());
        self.create_with_config(config)
    }

    fn create_with_config(
        &mut self,
        config: <Self::Backend as BackendConfig>::Config,
    ) -> Result<Self::Backend, Self::Error> {
        let (tx, rx) = mpsc::channel(config.batch_size);
        let mut r = self
            .registry
            .try_lock()
            .ok_or(SharedSqliteError::RegistryLocked)?;
        if r.insert(config.queue.to_string(), tx).is_some() {
            return Err(SharedSqliteError::NamespaceExists(config.queue.to_string()));
        }
        Ok(Interleave::new(
            SqliteStorage::new(&self.pool).with_config(config),
            SharedFetcher {
                poller: self.drive.clone(),
                receiver: Arc::new(Mutex::new(rx)),
            },
        ))
    }
}

/// A fetcher that uses a channel to receive jobs from a shared polling point
#[derive(Clone, Debug)]
pub struct SharedFetcher {
    poller: Shared<BoxFuture<'static, ()>>,
    receiver: Arc<Mutex<Receiver<Result<SqliteTask, Error>>>>,
}

impl Stream for SharedFetcher {
    type Item = Result<SqliteTask, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Keep the poller alive by polling it, but ignoring the output
        let _ = this.poller.poll_unpin(cx);

        let mut guard = ready!(this.receiver.lock().poll_unpin(cx));
        guard.poll_next_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis_core::{
        backend::TaskSink,
        error::BoxDynError,
        task::task_id::TaskId,
        worker::{builder::WorkerBuilder, context::WorkerContext},
    };

    use super::*;

    #[tokio::test]
    async fn factory_worker() {
        let mut factory = SqliteStorageFactory::new(":memory:");
        SqliteStorage::setup(factory.pool()).await.unwrap();

        let mut map_store = factory.create().unwrap();

        let mut int_store: SharedSqliteStorage<usize> = factory.create().unwrap();

        map_store
            .push(HashMap::<String, i32>::from([("value".to_string(), 42)]))
            .await
            .unwrap();
        int_store.push(99).await.unwrap();

        async fn send_reminder<T>(
            _: T,
            _task_id: TaskId,
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
