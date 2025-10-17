use apalis_core::{
    backend::{Backend, FetchById, codec::Codec},
    task::task_id::TaskId,
};
use ulid::Ulid;

use crate::{CompactType, SqliteContext, SqliteStorage, SqliteTask, from_row::TaskRow};

impl<Args, D, F> FetchById<Args> for SqliteStorage<Args, D, F>
where
    SqliteStorage<Args, D, F>:
        Backend<Context = SqliteContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
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
                .map(|r| r.try_into_task::<D, _>())
                .transpose()?;
            Ok(task)
        }
    }
}
