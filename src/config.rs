use apalis_core::backend::{BackendExt, ConfigExt, queue::Queue};
use ulid::Ulid;

use crate::{CompactType, SqlContext, SqliteStorage};

pub use apalis_sql::config::*;

impl<Args: Sync, D, F> ConfigExt for SqliteStorage<Args, D, F>
where
    Self:
        BackendExt<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
{
    fn get_queue(&self) -> Queue {
        self.config().queue().clone()
    }
}
