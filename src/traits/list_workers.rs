use apalis_core::{
    backend::{Filter, ListTasks, ListWorkers, RunningWorker, codec::json::JsonCodec},
    task::status::Status,
};
use futures::TryFutureExt;
use serde::{Serialize, de::DeserializeOwned};

use crate::{SqliteStorage, SqliteTask, from_row::TaskRow};

struct Worker {
    id: String,
    #[allow(unused)]
    worker_type: String,
    storage_name: String,
    layers: Option<String>,
    last_seen: i64,
}

impl<Args: Sync> ListWorkers<Args> for SqliteStorage<Args>
where
    Args: Send + 'static + Unpin + Serialize + DeserializeOwned,
{
    fn list_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let namespace = self.config.namespace().to_string();
        let pool = self.pool.clone();
        let limit = 100;
        let offset = 0;
        let pid = std::process::id() as i64;
        async move {
            let workers = sqlx::query_file_as!(
                Worker,
                "queries/backend/list_workers.sql",
                namespace,
                limit,
                offset
            )
            .fetch_all(&pool)
            .map_ok(|w| {
                w.into_iter()
                    .map(|w| RunningWorker {
                        id: w.id,
                        pid: pid as u32,
                        hostname: w.storage_name,
                        started_at: 0,
                        last_heartbeat: w.last_seen as u64,
                        service: w.layers.unwrap_or_default(),
                    })
                    .collect()
            })
            .await?;
            Ok(workers)
        }
    }
}

impl<Args: Send + Unpin + Serialize + DeserializeOwned + 'static> ListTasks<Args>
    for SqliteStorage<Args>
{
    fn list_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<SqliteTask<Args>>, Self::Error>> + Send {
        let namespace = self.config.namespace().to_string();
        let pool = self.pool.clone();
        let limit = filter.limit() as i32;
        let offset = filter.offset() as i32;
        let status = filter
            .status
            .as_ref()
            .unwrap_or(&Status::Pending)
            .to_string();
        async move {
            let tasks = sqlx::query_file_as!(
                TaskRow,
                "queries/backend/list_jobs.sql",
                status,
                namespace,
                limit,
                offset
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| r.try_into_task::<JsonCodec<String>, _>())
            .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        }
    }
}
