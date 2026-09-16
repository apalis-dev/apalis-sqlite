use std::{collections::HashSet, str::FromStr, vec};

use apalis_core::{
    backend::{Backend, TaskResult, WaitForCompletion},
    task::{
        status::Status,
        task_id::{TaskId, coalesce_ids},
    },
};
use futures::{StreamExt, stream::BoxStream};

use crate::{Error, SqliteStorage};

#[derive(Debug)]
struct ResultRow {
    pub id: Option<String>,
    pub status: Option<String>,
    pub result: Option<String>,
    pub attempt: Option<i64>,
}

impl<Args, O> WaitForCompletion<O> for SqliteStorage<Args>
where
    Self: Backend<Error = Error>,
    O: Send + 'static + serde::de::DeserializeOwned,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<O>, Self::Error>>;
    fn wait_for(&self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        let pool = self.persistence.pool.clone();
        let ids: HashSet<String> = task_ids.into_iter().map(|id| id.to_string()).collect();

        let stream = futures::stream::unfold(ids, move |mut remaining_ids| {
            let pool = pool.clone();
            async move {
                if remaining_ids.is_empty() {
                    return None;
                }

                let ids_vec: Vec<String> = remaining_ids.iter().cloned().collect();
                let ids_vec = coalesce_ids(ids_vec);
                let rows = sqlx::query_file_as!(
                    ResultRow,
                    "queries/backend/fetch_completed_tasks.sql",
                    ids_vec
                )
                .fetch_all(&pool)
                .await
                .ok()?;

                if rows.is_empty() {
                    apalis_core::timer::sleep(std::time::Duration::from_millis(500)).await;
                    return Some((futures::stream::iter(vec![]), remaining_ids));
                }

                let mut results = Vec::new();
                for row in rows {
                    let task_id = row.id.clone().unwrap();
                    remaining_ids.remove(&task_id);
                    let result: Result<O, String> =
                        serde_json::from_str(&row.result.unwrap()).unwrap();
                    results.push(Ok(TaskResult {
                        task_id: TaskId::from_str(&task_id).ok()?,
                        status: Status::from_str(&row.status.unwrap()).ok()?,
                        attempt: row.attempt.unwrap_or_default() as usize,
                        result,
                    }));
                }

                Some((futures::stream::iter(results), remaining_ids))
            }
        });
        stream.flatten().boxed()
    }

    // Implementation of check_status
    fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<O>>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();

        async move {
            let ids = coalesce_ids(task_ids);
            let rows =
                sqlx::query_file_as!(ResultRow, "queries/backend/fetch_completed_tasks.sql", ids)
                    .fetch_all(&pool)
                    .await?;

            let mut results = Vec::new();
            for row in rows {
                let task_id = TaskId::from_str(&row.id.unwrap()).map_err(Error::TaskIdError)?;

                let result: Result<O, String> =
                    serde_json::from_str(&row.result.unwrap()).map_err(Error::JsonError)?;

                results.push(TaskResult {
                    task_id,
                    status: row.status.unwrap().parse().map_err(Error::StatusError)?,
                    result,
                    attempt: row.attempt.unwrap_or_default() as usize,
                });
            }

            Ok(results)
        }
    }
}
