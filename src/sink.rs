use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Sink;
use sqlx::Executor;
use ulid::Ulid;

use crate::{SqliteStorage, SqliteTask, error::Error};

/// Push a batch of tasks into the database
pub async fn push_tasks<'a, E>(
    executor: &'a mut E,
    queue: &str,
    tasks: &[SqliteTask],
) -> Result<(), Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::Sqlite>,
{
    for task in tasks {
        let id = task
            .task_id()
            .as_ref()
            .map(|id| id.to_string())
            .unwrap_or(Ulid::generate().to_string());
        let run_at = task.run_at().unwrap_or(0) as i64;
        let max_attempts = task.max_attempts().unwrap_or(25) as i64;
        let priority = task.priority().unwrap_or_default() as i64;
        let args = &task.args;
        let idempotency_key = task.idempotency_key();
        let meta = serde_json::to_string(task.metadata()).unwrap_or_default();
        sqlx::query_file!(
            "queries/task/sink.sql",
            args,
            id,
            queue,
            max_attempts,
            run_at,
            priority,
            meta,
            idempotency_key
        )
        .execute(&mut *executor)
        .await?;
    }
    Ok(())
}

impl<Args> Sink<SqliteTask> for SqliteStorage<Args> {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: SqliteTask) -> Result<(), Self::Error> {
        self.project().persistence.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_flush(self.project().persistence, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_close(self.project().persistence, cx)
    }
}
