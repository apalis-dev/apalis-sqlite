use apalis_core::{
    backend::{Backend, FetchById, codec::json::JsonCodec},
    task::task_id::TaskId,
};
use serde::{Serialize, de::DeserializeOwned};
use ulid::Ulid;

use crate::{SqliteContext, SqliteStorage, SqliteTask, from_row::TaskRow};

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
