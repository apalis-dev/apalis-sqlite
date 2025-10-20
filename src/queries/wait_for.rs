use std::{collections::HashSet, str::FromStr, vec};

use apalis_core::{
    backend::{Backend, TaskResult, WaitForCompletion},
    task::{status::Status, task_id::TaskId},
};
use futures::{StreamExt, stream::BoxStream};
use serde::de::DeserializeOwned;
use ulid::Ulid;

use crate::{CompactType, SqliteStorage};

#[derive(Debug)]
struct ResultRow {
    pub id: Option<String>,
    pub status: Option<String>,
    pub result: Option<CompactType>,
}

impl<O: 'static + Send, Args, F, Decode> WaitForCompletion<O> for SqliteStorage<Args, Decode, F>
where
    SqliteStorage<Args, Decode, F>:
        Backend<IdType = Ulid, Codec = Decode, Error = sqlx::Error, Compact = CompactType>,
    Result<O, String>: DeserializeOwned,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<O>, Self::Error>>;
    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream {
        let pool = self.pool.clone();
        let ids: HashSet<String> = task_ids.into_iter().map(|id| id.to_string()).collect();

        let stream = futures::stream::unfold(ids, move |mut remaining_ids| {
            let pool = pool.clone();
            async move {
                if remaining_ids.is_empty() {
                    return None;
                }

                let ids_vec: Vec<String> = remaining_ids.iter().cloned().collect();
                let ids_vec = serde_json::to_string(&ids_vec).unwrap();
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
                    // Here we would normally decode the output O from the row
                    // For simplicity, we assume O is String and the output is stored in row.output
                    let result: Result<O, String> =
                        serde_json::from_str(&row.result.unwrap()).unwrap();
                    results.push(Ok(TaskResult::new(
                        TaskId::from_str(&task_id).ok()?,
                        Status::from_str(&row.status.unwrap()).ok()?,
                        result,
                    )));
                }

                Some((futures::stream::iter(results), remaining_ids))
            }
        });
        stream.flatten().boxed()
    }

    // Implementation of check_status
    fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<O>>, Self::Error>> + Send {
        let pool = self.pool.clone();
        let ids: Vec<String> = task_ids.into_iter().map(|id| id.to_string()).collect();

        async move {
            let ids = serde_json::to_string(&ids).unwrap();
            let rows =
                sqlx::query_file_as!(ResultRow, "queries/backend/fetch_completed_tasks.sql", ids)
                    .fetch_all(&pool)
                    .await?;

            let mut results = Vec::new();
            for row in rows {
                let task_id = TaskId::from_str(&row.id.unwrap())
                    .map_err(|_| sqlx::Error::Protocol("Invalid task ID".into()))?;

                let result: Result<O, String> = serde_json::from_str(&row.result.unwrap())
                    .map_err(|_| sqlx::Error::Protocol("Failed to decode result".into()))?;

                results.push(TaskResult::new(
                    task_id,
                    row.status
                        .unwrap()
                        .parse()
                        .map_err(|_| sqlx::Error::Protocol("Invalid status value".into()))?,
                    result,
                ));
            }

            Ok(results)
        }
    }
}
