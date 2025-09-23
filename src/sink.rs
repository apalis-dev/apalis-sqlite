use std::{
    pin::{self, Pin},
    task::{Context, Poll},
};

use apalis_core::{
    backend::codec::{Codec, json::JsonCodec},
    error::BoxDynError,
};
use chrono::DateTime;
use futures::Sink;
use serde_json::Value;
use sqlx::SqlitePool;
use ulid::Ulid;

use crate::{SqliteStorage, SqliteTask, config::Config};

#[pin_project::pin_project]
pub struct SqliteSink<Args, Compact = String, Codec = JsonCodec<String>> {
    pool: SqlitePool,
    config: Config,
    buffer: Vec<SqliteTask<Compact>>,
    #[pin]
    flush_future: Option<Pin<Box<dyn Future<Output = Result<(), sqlx::Error>> + Send>>>,
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

pub(crate) async fn push_tasks(
    pool: SqlitePool,
    cfg: Config,
    buffer: Vec<SqliteTask<String>>,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let job_type = cfg.namespace();

    for task in buffer {
        let id = task
            .parts
            .task_id
            .map(|id| id.to_string())
            .unwrap_or(Ulid::new().to_string());
        let run_at = task.parts.run_at as i64;
        let max_attempts = task.parts.ctx.max_attempts() as i32;
        let priority = task.parts.ctx.priority() as i32;
        let args = task.args;
        sqlx::query_file!(
            "queries/task/sink.sql",
            args,
            id,
            job_type,
            max_attempts,
            run_at,
            priority,
        )
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(())
}

impl<Args, Compact, Codec> SqliteSink<Args, Compact, Codec> {
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

impl<Args, Encode, Fetcher> Sink<SqliteTask<Args>> for SqliteStorage<Args, Encode, Fetcher>
where
    Args:  Send + Sync + 'static,
    Encode: Codec<Args, Compact = String>,
    Encode::Error: std::error::Error + Send + Sync + 'static,
    Encode::Error: Into<BoxDynError>,
{
    type Error = sqlx::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: SqliteTask<Args>) -> Result<(), Self::Error> {
        // Add the item to the buffer
        self.project()
            .sink
            .buffer
            .push(item.try_map(|s| Encode::encode(&s).map_err(|e| sqlx::Error::Encode(e.into())))?);
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
            this.sink.flush_future = Some(Box::pin(sink_fut));
        }

        // Poll the existing future
        if let Some(mut fut) = this.sink.flush_future.take() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    // Future completed successfully, don't put it back
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    // Future completed with error, don't put it back
                    Poll::Ready(Err(e))
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
