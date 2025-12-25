use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::AtomicUsize},
    task::{Context, Poll},
};

use apalis_core::{
    backend::poll_strategy::{PollContext, PollStrategyExt},
    task::Task,
    worker::context::WorkerContext,
};
use apalis_sql::from_row::TaskRow;

use crate::SqliteDateTime;
use futures::{FutureExt, future::BoxFuture, stream::Stream};
use pin_project::pin_project;
use sqlx::{Pool, Sqlite, SqlitePool};
use ulid::Ulid;

use crate::{CompactType, Config, SqliteContext, SqliteTask, from_row::SqliteTaskRow};

/// Fetch the next batch of tasks from the sqlite backend
pub async fn fetch_next(
    pool: SqlitePool,
    config: Config,
    worker: WorkerContext,
) -> Result<Vec<Task<CompactType, SqliteContext, Ulid>>, sqlx::Error>
where
{
    let job_type = config.queue().to_string();
    let buffer_size = config.buffer_size() as i32;
    let worker = worker.name().clone();
    sqlx::query_file_as!(
        SqliteTaskRow,
        "queries/backend/fetch_next.sql",
        worker,
        job_type,
        buffer_size
    )
    .fetch_all(&pool)
    .await?
    .into_iter()
    .map(|r| {
        let row: TaskRow<SqliteDateTime> = r.try_into()?;
        row.try_into_task_compact()
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))
    })
    .collect()
}

enum StreamState {
    Ready,
    Delay,
    Fetch(BoxFuture<'static, Result<Vec<SqliteTask<CompactType>>, sqlx::Error>>),
    Buffered(VecDeque<SqliteTask<CompactType>>),
    Empty,
}

/// Dispatcher for fetching tasks from a SQLite backend via [SqlitePollFetcher]
#[derive(Clone, Debug)]
pub struct SqliteFetcher;
/// Polling-based fetcher for retrieving tasks from a SQLite backend
#[pin_project]
pub struct SqlitePollFetcher<Compact, Decode> {
    pool: SqlitePool,
    config: Config,
    wrk: WorkerContext,
    _marker: PhantomData<(Compact, Decode)>,
    #[pin]
    state: StreamState,

    #[pin]
    delay_stream: Option<Pin<Box<dyn Stream<Item = ()> + Send>>>,

    prev_count: Arc<AtomicUsize>,
}

impl<Compact, Decode> std::fmt::Debug for SqlitePollFetcher<Compact, Decode> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlitePollFetcher")
            .field("pool", &self.pool)
            .field("config", &self.config)
            .field("wrk", &self.wrk)
            .field("_marker", &self._marker)
            .field("prev_count", &self.prev_count)
            .finish()
    }
}

impl<Compact, Decode> Clone for SqlitePollFetcher<Compact, Decode> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            wrk: self.wrk.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
            delay_stream: None,
            prev_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<Decode> SqlitePollFetcher<CompactType, Decode> {
    /// Create a new SqlitePollFetcher
    #[must_use]
    pub fn new(pool: &Pool<Sqlite>, config: &Config, wrk: &WorkerContext) -> Self {
        Self {
            pool: pool.clone(),
            config: config.clone(),
            wrk: wrk.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
            delay_stream: None,
            prev_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<Decode> Stream for SqlitePollFetcher<CompactType, Decode> {
    type Item = Result<Option<SqliteTask<CompactType>>, sqlx::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.delay_stream.is_none() {
            let strategy = this
                .config
                .poll_strategy()
                .clone()
                .build_stream(&PollContext::new(this.wrk.clone(), this.prev_count.clone()));
            this.delay_stream = Some(Box::pin(strategy));
        }

        loop {
            match this.state {
                StreamState::Ready => {
                    let stream =
                        fetch_next(this.pool.clone(), this.config.clone(), this.wrk.clone());
                    this.state = StreamState::Fetch(stream.boxed());
                }
                StreamState::Delay => {
                    if let Some(delay_stream) = this.delay_stream.as_mut() {
                        match delay_stream.as_mut().poll_next(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Some(_)) => {
                                this.state = StreamState::Ready;
                            }
                            Poll::Ready(None) => {
                                this.state = StreamState::Empty;
                                return Poll::Ready(None);
                            }
                        }
                    } else {
                        this.state = StreamState::Empty;
                        return Poll::Ready(None);
                    }
                }

                StreamState::Fetch(ref mut fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(item) => match item {
                        Ok(requests) => {
                            if requests.is_empty() {
                                this.state = StreamState::Delay;
                            } else {
                                let mut buffer = VecDeque::new();
                                for request in requests {
                                    buffer.push_back(request);
                                }

                                this.state = StreamState::Buffered(buffer);
                            }
                        }
                        Err(e) => {
                            this.state = StreamState::Empty;
                            return Poll::Ready(Some(Err(e)));
                        }
                    },
                },

                StreamState::Buffered(ref mut buffer) => {
                    if let Some(request) = buffer.pop_front() {
                        // Yield the next buffered item
                        if buffer.is_empty() {
                            // Buffer is now empty, transition to ready for next fetch
                            this.state = StreamState::Ready;
                        }
                        return Poll::Ready(Some(Ok(Some(request))));
                    } else {
                        // Buffer is empty, transition to ready
                        this.state = StreamState::Ready;
                    }
                }

                StreamState::Empty => return Poll::Ready(None),
            }
        }
    }
}

impl<Compact, Decode> SqlitePollFetcher<Compact, Decode> {
    /// Take pending tasks from the fetcher
    pub fn take_pending(&mut self) -> VecDeque<SqliteTask<Vec<u8>>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}
