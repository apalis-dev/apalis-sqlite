use apalis_core::backend::{BackendExt, Vacuum};
use ulid::Ulid;

use crate::{CompactType, SqliteStorage};

impl<Args, F, Decode> Vacuum for SqliteStorage<Args, Decode, F>
where
    Self: BackendExt<IdType = Ulid, Codec = Decode, Error = sqlx::Error, Compact = CompactType>,
    F: Send,
    Decode: Send,
    Args: Send,
{
    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        let res = sqlx::query_file!("queries/backend/vacuum.sql")
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() as usize)
    }
}
