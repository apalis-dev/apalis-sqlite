use apalis_core::{
    backend::{
        Backend, FetchById, Filter, ListAllTasks, ListQueues, ListTasks, ListWorkers, Metrics,
        QueueInfo, RunningWorker, StatType, Statistic,
        codec::{Codec, json::JsonCodec},
    },
    task::{Task, status::Status, task_id::TaskId},
};
use futures::TryFutureExt;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::Decode;
use ulid::Ulid;

use crate::{SqliteContext, SqliteStorage, SqliteTask, from_row::TaskRow};

struct Worker {
    id: String,
    worker_type: String,
    storage_name: String,
    layers: Option<String>,
    last_seen: i64,
}

impl<Args: Sync, D, F> ListWorkers for SqliteStorage<Args, D, F>
where
    SqliteStorage<Args, D, F>:
        Backend<Context = SqliteContext, Compact = String, IdType = Ulid, Error = sqlx::Error>,
{
    fn list_workers(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let queue = queue.to_string();
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
                        started_at: 0,
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
                        started_at: 0,
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

impl<Args, D, F> ListTasks<Args> for SqliteStorage<Args, D, F>
where
    SqliteStorage<Args, D, F>:
        Backend<Context = SqliteContext, Compact = String, IdType = Ulid, Error = sqlx::Error>,
    Args: Serialize + DeserializeOwned,
{
    fn list_tasks(
        &self,
        queue: &str,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<SqliteTask<Args>>, Self::Error>> + Send {
        let queue = queue.to_string();
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
                queue,
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

impl<Args, D, F> ListAllTasks for SqliteStorage<Args, D, F>
where
    SqliteStorage<Args, D, F>:
        Backend<Context = SqliteContext, Compact = String, IdType = Ulid, Error = sqlx::Error>,
{
    fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<
        Output = Result<Vec<Task<Self::Compact, Self::Context, Self::IdType>>, Self::Error>,
    > + Send {
        let status = filter
            .status
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or(Status::Pending.to_string());
        let pool = self.pool.clone();
        let limit = filter.limit() as i32;
        let offset = filter.offset() as i32;
        async move {
            let tasks = sqlx::query_file_as!(
                TaskRow,
                "queries/backend/list_all_jobs.sql",
                status,
                limit,
                offset
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| r.try_into_task_compact())
            .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        }
    }
}

struct QueueInfoRow {
    name: String,
    stats: String,    // JSON string
    workers: String,  // JSON string
    activity: String, // JSON string
}

impl From<QueueInfoRow> for QueueInfo {
    fn from(row: QueueInfoRow) -> Self {
        QueueInfo {
            name: row.name,
            stats: serde_json::from_str(&row.stats).unwrap(),
            workers: serde_json::from_str(&row.workers).unwrap(),
            activity: serde_json::from_str(&row.activity).unwrap(),
        }
    }
}

impl<Args, D, F> ListQueues for SqliteStorage<Args, D, F>
where
    SqliteStorage<Args, D, F>:
        Backend<Context = SqliteContext, Compact = String, IdType = Ulid, Error = sqlx::Error>,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let pool = self.pool.clone();

        async move {
            let queues = sqlx::query_file_as!(QueueInfoRow, "queries/backend/list_queues.sql")
                .fetch_all(&pool)
                .await?
                .into_iter()
                .map(QueueInfo::from)
                .collect();
            Ok(queues)
        }
    }
}

fn stat_type_from_string(s: &str) -> StatType {
    match s {
        "Number" => StatType::Number,
        "Decimal" => StatType::Decimal,
        "Percentage" => StatType::Percentage,
        "Timestamp" => StatType::Timestamp,
        _ => StatType::Number, // default fallback
    }
}
struct StatisticRow {
    /// The priority of the statistic (lower number means higher priority)
    pub priority: i64,
    /// The statistics type
    pub r#type: String,
    /// Overall statistics of the backend
    pub statistic: String,
    /// The value of the statistic
    pub value: Option<f64>,
}

impl<Args, D, F> Metrics for SqliteStorage<Args, D, F>
where
    SqliteStorage<Args, D, F>:
        Backend<Context = SqliteContext, Compact = String, IdType = Ulid, Error = sqlx::Error>,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let pool = self.pool.clone();
        async move {
            let rec = sqlx::query_file_as!(StatisticRow, "queries/backend/overview.sql")
                .fetch_all(&pool)
                .await?
                .into_iter()
                .map(|r| Statistic {
                    priority: Some(r.priority as u64),
                    stat_type: stat_type_from_string(&r.r#type),
                    title: r.statistic,
                    value: r.value.unwrap_or_default().to_string(),
                })
                .collect();
            Ok(rec)
        }
    }
    fn fetch_by_queue(
        &self,
        queue_id: &str,
    ) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let pool = self.pool.clone();
        let queue_id = queue_id.to_string();
        async move {
            let rec = sqlx::query_file_as!(
                StatisticRow,
                "queries/backend/overview_by_queue.sql",
                queue_id
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| Statistic {
                priority: Some(r.priority as u64),
                stat_type: stat_type_from_string(&r.r#type),
                title: r.statistic,
                value: r.value.unwrap_or_default().to_string(),
            })
            .collect();
            Ok(rec)
        }
    }
}

impl<Args, D, F> FetchById<Args> for SqliteStorage<Args, D, F>
where
    SqliteStorage<Args, D, F>:
        Backend<Context = SqliteContext, Compact = String, IdType = Ulid, Error = sqlx::Error>,
    Args: Serialize + DeserializeOwned,
{
    fn fetch_by_id(
        &mut self,
        id: &TaskId<Ulid>,
    ) -> impl Future<Output = Result<Option<SqliteTask<Args>>, Self::Error>> + Send {
        let pool = self.pool.clone();
        let id = id.to_string();
        async move {
            let task = sqlx::query_file_as!(TaskRow, "queries/task/find_by_id.sql", id)
                .fetch_optional(&pool)
                .await?
                .map(|r| r.try_into_task::<JsonCodec<String>, _>())
                .transpose()?;
            Ok(task)
        }
    }
}
