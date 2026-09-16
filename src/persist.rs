use apalis_core::{
    backend::{
        WorkerFilter,
        persistence::{Persistence, TaskEvent},
    },
    task::Task,
    worker::context::WorkerContext,
};
use serde_json::Value;
use sqlx::SqlitePool;

use crate::{
    error::Error,
    queries::{
        self, fetch_next, keep_alive, push_tasks, reenqueue_abandoned, reenqueue_orphaned,
        register_worker,
    },
};

use crate::config::Config;

#[derive(Debug, Clone)]
pub(crate) struct SqlxPersistence {
    pub(crate) pool: SqlitePool,
    pub(crate) config: Config,
}

impl Persistence for SqlxPersistence {
    type Compact = Vec<u8>;
    type Error = Error;
    type Response = Value;
    async fn register(&mut self, worker: &WorkerContext) -> Result<(), Error> {
        let dead_for = self.config.heartbeat_interval.as_secs() as i64;
        let mut tx = self.pool.begin().await?;
        let count = reenqueue_orphaned(
            &mut *tx,
            dead_for,
            self.config.queue.as_ref(),
            &WorkerFilter::Only(worker.name().to_owned()),
        )
        .await?;
        if count > 0 {
            log::debug!(
                "{count} Re-enqueued orphaned tasks by worker {}",
                worker.name()
            );
        }
        register_worker(&mut *tx, &self.config, worker, "SqliteStorage").await?;
        tx.commit().await?;
        log::debug!("Registered Worker: {}", worker.name());
        Ok(())
    }
    async fn heartbeat(&mut self, worker: &WorkerContext) -> Result<(), Error> {
        let config = &self.config;
        let mut txn = self.pool.begin().await?;
        keep_alive(&mut *txn, config, worker).await?;
        let count = reenqueue_orphaned(
            &mut *txn,
            config.orphaned_duration().as_secs() as i64,
            config.queue.as_ref(),
            &WorkerFilter::AllExcept(worker.name().to_owned()),
        )
        .await?;
        txn.commit().await?;
        if count > 0 {
            log::debug!(
                "Re-enqueued {count} orphaned tasks by worker {}",
                worker.name()
            );
        }
        Ok(())
    }
    async fn fetch_next(&mut self, worker: &WorkerContext) -> Result<Vec<Task<Vec<u8>>>, Error> {
        let mut tx = self.pool.begin().await?;
        let res = fetch_next(&mut *tx, &self.config, worker).await?;
        tx.commit().await?;
        Ok(res)
    }

    async fn handle_events(
        &mut self,
        messages: Vec<TaskEvent<Self::Response>>,
        worker: &WorkerContext,
    ) -> Result<(), Error> {
        let pool = &self.pool;
        let lock_ids = messages
            .iter()
            .filter_map(|msg| {
                if let TaskEvent::Lock { task_id, .. } = msg {
                    Some(task_id.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let ack_payloads = messages
            .iter()
            .filter_map(|msg| {
                if let TaskEvent::Complete(payload) = msg {
                    Some(payload)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if lock_ids.is_empty() && ack_payloads.is_empty() {
            return Ok(());
        }

        log::debug!(
            "Processing {} messages ({} locks, {} acks)",
            messages.len(),
            lock_ids.len(),
            ack_payloads.len()
        );

        let mut tx = pool.begin().await?;
        if !lock_ids.is_empty() {
            queries::lock_tasks(&mut *tx, &lock_ids, worker.name()).await?;
        }
        if !ack_payloads.is_empty() {
            queries::ack_tasks(&mut *tx, &ack_payloads, worker.name()).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn reenqueue_abandoned(
        &mut self,
        tasks: Vec<Task<Vec<u8>>>,
        worker: &WorkerContext,
    ) -> Result<u64, Error> {
        let config = &self.config;
        let pool = &self.pool;
        let mut txn = pool.begin().await?;
        let task_ids = tasks
            .iter()
            .map(|t| t.task_id().unwrap().to_string())
            .collect();
        let count =
            reenqueue_abandoned(&mut *txn, config.queue.as_ref(), worker.name(), &task_ids).await?;
        if count as usize != tasks.len() {
            return Err(Error::ReenqueueMismatch {
                queued: tasks.len(),
                abandoned: count as usize,
            });
        }
        txn.commit().await?;
        Ok(count)
    }
    async fn push_tasks(&mut self, tasks: Vec<Task<Self::Compact>>) -> Result<(), Self::Error> {
        let queue = self.config.queue.as_ref();
        let mut tx = self.pool.begin().await?;
        push_tasks(&mut tx, queue, &tasks).await?;
        tx.commit().await?;
        Ok(())
    }
}
