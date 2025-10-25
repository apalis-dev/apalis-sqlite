use apalis_core::{
    backend::{Backend, Filter, ListAllTasks, ListTasks, codec::Codec},
    task::{Task, status::Status},
};
use apalis_sql::from_row::TaskRow;
use ulid::Ulid;

use crate::{CompactType, SqlContext, SqliteStorage, SqliteTask, from_row::SqliteTaskRow};

impl<Args, D, F> ListTasks<Args> for SqliteStorage<Args, D, F>
where
    SqliteStorage<Args, D, F>:
        Backend<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
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
                SqliteTaskRow,
                "queries/backend/list_jobs.sql",
                status,
                queue,
                limit,
                offset
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| {
                let row: TaskRow = r.try_into()?;
                row.try_into_task::<D, _, Ulid>()
                    .map_err(|e| sqlx::Error::Protocol(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        }
    }
}

impl<Args, D, F> ListAllTasks for SqliteStorage<Args, D, F>
where
    SqliteStorage<Args, D, F>:
        Backend<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
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
                SqliteTaskRow,
                "queries/backend/list_all_jobs.sql",
                status,
                limit,
                offset
            )
            .fetch_all(&pool)
            .await?
            .into_iter()
            .map(|r| {
                let row: TaskRow = r.try_into()?;
                row.try_into_task_compact()
                    .map_err(|e| sqlx::Error::Protocol(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        }
    }
}
