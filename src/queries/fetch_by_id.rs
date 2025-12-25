use apalis_core::{
    backend::{BackendExt, FetchById, codec::Codec},
    task::task_id::TaskId,
};
use apalis_sql::from_row::{FromRowError, TaskRow};
use ulid::Ulid;

use crate::{CompactType, SqliteContext, SqliteStorage, SqliteTask, from_row::SqliteTaskRow};

impl<Args, D, F> FetchById<Args> for SqliteStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SqliteContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = sqlx::Error,
        >,
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
{
    fn fetch_by_id(
        &mut self,
        id: &TaskId<Ulid>,
    ) -> impl Future<Output = Result<Option<SqliteTask<Args>>, Self::Error>> + Send {
        let pool = self.pool.clone();
        let id = id.to_string();
        async move {
            let task = sqlx::query_file_as!(SqliteTaskRow, "queries/task/find_by_id.sql", id)
                .fetch_optional(&pool)
                .await?
                .map(|r| {
                    let row: TaskRow = r
                        .try_into()
                        .map_err(|e: sqlx::Error| FromRowError::DecodeError(e.into()))?;
                    row.try_into_task_compact().and_then(|t| {
                        t.try_map(|a| {
                            D::decode(&a).map_err(|e| FromRowError::DecodeError(e.into()))
                        })
                    })
                })
                .transpose()
                .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            Ok(task)
        }
    }
}
