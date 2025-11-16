use apalis_core::backend::{BackendExt, ListWorkers, RunningWorker};
use futures::TryFutureExt;
use ulid::Ulid;

use crate::{CompactType, SqlContext, SqliteStorage};

struct Worker {
    id: String,
    worker_type: String,
    storage_name: String,
    layers: Option<String>,
    last_seen: i64,
    started_at: Option<i64>,
}

impl<Args: Sync, D, F> ListWorkers for SqliteStorage<Args, D, F>
where
    Self:
        BackendExt<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
{
    fn list_workers(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let queue = queue.to_owned();
        let pool = self.pool.clone();
        let limit = 100;
        let offset = 0;
        async move {
            let workers = sqlx::query_file_as!(
                Worker,
                "queries/backend/list_workers.sql",
                queue,
                limit,
                offset
            )
            .fetch_all(&pool)
            .map_ok(|w| {
                w.into_iter()
                    .map(|w| RunningWorker {
                        id: w.id,
                        backend: w.storage_name,
                        started_at: w.started_at.unwrap_or_default() as u64,
                        last_heartbeat: w.last_seen as u64,
                        layers: w.layers.unwrap_or_default(),
                        queue: w.worker_type,
                    })
                    .collect()
            })
            .await?;
            Ok(workers)
        }
    }

    fn list_all_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let pool = self.pool.clone();
        let limit = 100;
        let offset = 0;
        async move {
            let workers = sqlx::query_file_as!(
                Worker,
                "queries/backend/list_all_workers.sql",
                limit,
                offset
            )
            .fetch_all(&pool)
            .map_ok(|w| {
                w.into_iter()
                    .map(|w| RunningWorker {
                        id: w.id,
                        backend: w.storage_name,
                        started_at: w.started_at.unwrap_or_default() as u64,
                        last_heartbeat: w.last_seen as u64,
                        layers: w.layers.unwrap_or_default(),
                        queue: w.worker_type,
                    })
                    .collect()
            })
            .await?;
            Ok(workers)
        }
    }
}
