use std::str::FromStr;

use apalis_core::backend::{Backend, Metrics, StatType, Statistic};

use crate::{SqliteStorage, error::Error};

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

impl<Args> Metrics for SqliteStorage<Args>
where
    Self: Backend<Error = Error>,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();
        async move {
            let rec = sqlx::query_file_as!(StatisticRow, "queries/backend/overview.sql")
                .fetch_all(&pool)
                .await?
                .into_iter()
                .map(|r| Statistic {
                    priority: Some(r.priority as u64),
                    stat_type: FromStr::from_str(&r.r#type).unwrap_or(StatType::Number),
                    title: r.statistic,
                    value: r.value.unwrap_or_default().to_string(),
                })
                .collect();
            Ok(rec)
        }
    }
    fn fetch_by_queue(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();
        let queue_id = self.persistence.config.queue.as_ref();
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
                stat_type: FromStr::from_str(&r.r#type).unwrap_or(StatType::Number),
                title: r.statistic,
                value: r.value.unwrap_or_default().to_string(),
            })
            .collect();
            Ok(rec)
        }
    }
}
