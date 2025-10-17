use apalis_core::backend::{Backend, Metrics, Statistic};
use ulid::Ulid;

use crate::{CompactType, SqliteContext, SqliteStorage};

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
        Backend<Context = SqliteContext, Compact = CompactType, IdType = Ulid, Error = sqlx::Error>,
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
                    stat_type: super::stat_type_from_string(&r.r#type),
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
                stat_type: super::stat_type_from_string(&r.r#type),
                title: r.statistic,
                value: r.value.unwrap_or_default().to_string(),
            })
            .collect();
            Ok(rec)
        }
    }
}
