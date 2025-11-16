use apalis_core::backend::{BackendExt, ListQueues, QueueInfo};
use ulid::Ulid;

use crate::{CompactType, SqlContext, SqliteStorage};

struct QueueInfoRow {
    name: String,
    stats: String,    // JSON string
    workers: String,  // JSON string
    activity: String, // JSON string
}

impl From<QueueInfoRow> for QueueInfo {
    fn from(row: QueueInfoRow) -> Self {
        Self {
            name: row.name,
            stats: serde_json::from_str(&row.stats).unwrap(),
            workers: serde_json::from_str(&row.workers).unwrap(),
            activity: serde_json::from_str(&row.activity).unwrap(),
        }
    }
}

impl<Args, D, F> ListQueues for SqliteStorage<Args, D, F>
where
    Self:
        BackendExt<Context = SqlContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
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
