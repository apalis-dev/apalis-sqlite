use apalis_core::backend::{Backend, ListQueues, QueueInfo};

use crate::{SqliteStorage, error::Error};

struct QueueInfoRow {
    name: String,
    stats: Option<String>,    // JSON string
    workers: Option<String>,  // JSON string
    activity: Option<String>, // JSON string
}

impl From<QueueInfoRow> for QueueInfo {
    fn from(row: QueueInfoRow) -> Self {
        Self {
            name: row.name,
            stats: row
                .stats
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            workers: row
                .workers
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            activity: row
                .activity
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
        }
    }
}

impl<Args> ListQueues for SqliteStorage<Args>
where
    Self: Backend<Error = Error>,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();

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
