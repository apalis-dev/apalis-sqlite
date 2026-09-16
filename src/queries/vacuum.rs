use apalis_core::backend::{Backend, Vacuum};

use crate::{Error, SqliteStorage};

impl<Args> Vacuum for SqliteStorage<Args>
where
    Self: Backend<Error = Error>,
    Args: Send,
{
    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        let res = sqlx::query_file!("queries/backend/vacuum.sql")
            .execute(&self.persistence.pool)
            .await?;
        Ok(res.rows_affected() as usize)
    }
}
