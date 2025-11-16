use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{
    FutureExt, Sink,
    future::{BoxFuture, Shared},
};
use sqlx::SqlitePool;
use ulid::Ulid;

use crate::{CompactType, SqliteStorage, SqliteTask, config::Config};

type FlushFuture = BoxFuture<'static, Result<(), Arc<sqlx::Error>>>;

/// Sink for pushing tasks into the sqlite backend
#[pin_project::pin_project]
#[derive(Debug)]
pub struct SqliteSink<Args, Compact, Codec> {
    pool: SqlitePool,
    config: Config,
    buffer: Vec<SqliteTask<Compact>>,
    #[pin]
    flush_future: Option<Shared<FlushFuture>>,
    _marker: std::marker::PhantomData<(Args, Codec)>,
}

impl<Args, Compact, Codec> Clone for SqliteSink<Args, Compact, Codec> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Push a batch of tasks into the database
pub async fn push_tasks(
    pool: SqlitePool,
    cfg: Config,
    buffer: Vec<SqliteTask<CompactType>>,
) -> Result<(), Arc<sqlx::Error>> {
    let mut tx = pool.begin().await?;
    for task in buffer {
        let id = task
            .parts
            .task_id
            .map(|id| id.to_string())
            .unwrap_or(Ulid::new().to_string());
        let run_at = task.parts.run_at as i64;
        let max_attempts = task.parts.ctx.max_attempts();
        let priority = task.parts.ctx.priority();
        let args = task.args;
        // Use specified queue if specified, otherwise use default
        let job_type = cfg.queue().to_string();
        let meta = serde_json::to_string(&task.parts.ctx.meta()).unwrap_or_default();
        sqlx::query_file!(
            "queries/task/sink.sql",
            args,
            id,
            job_type,
            max_attempts,
            run_at,
            priority,
            meta
        )
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(())
}

impl<Args, Compact, Codec> SqliteSink<Args, Compact, Codec> {
    /// Create a new SqliteSink
    #[must_use]
    pub fn new(pool: &SqlitePool, config: &Config) -> Self {
        Self {
            pool: pool.clone(),
            config: config.clone(),
            buffer: Vec::new(),
            _marker: std::marker::PhantomData,
            flush_future: None,
        }
    }
}

impl<Args, Encode, Fetcher> Sink<SqliteTask<CompactType>> for SqliteStorage<Args, Encode, Fetcher>
where
    Args: Send + Sync + 'static,
{
    type Error = sqlx::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: SqliteTask<CompactType>) -> Result<(), Self::Error> {
        // Add the item to the buffer
        self.project().sink.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();

        // If there's no existing future and buffer is empty, we're done
        if this.sink.flush_future.is_none() && this.sink.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Create the future only if we don't have one and there's work to do
        if this.sink.flush_future.is_none() && !this.sink.buffer.is_empty() {
            let pool = this.pool.clone();
            let config = this.config.clone();
            let buffer = std::mem::take(&mut this.sink.buffer);
            let sink_fut = push_tasks(pool, config, buffer);
            this.sink.flush_future = Some((Box::pin(sink_fut) as FlushFuture).shared());
        }

        // Poll the existing future
        if let Some(mut fut) = this.sink.flush_future.take() {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    // Future completed successfully, don't put it back
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    // Future completed with error, don't put it back
                    Poll::Ready(Err(Arc::into_inner(e).unwrap()))
                }
                Poll::Pending => {
                    // Future is still pending, put it back and return Pending
                    this.sink.flush_future = Some(fut);
                    Poll::Pending
                }
            }
        } else {
            // No future and no work to do
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
