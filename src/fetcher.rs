use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::AtomicUsize},
    task::{Context, Poll},
};

use apalis_core::{
    backend::{
        codec::{Codec, },
        poll_strategy::{PollContext, PollStrategyExt},
    },
    task::Task,
    worker::context::WorkerContext,
};
use futures::{FutureExt, future::BoxFuture, stream::Stream};
use pin_project::pin_project;
use sqlx::{Pool, Sqlite, SqlitePool};
use ulid::Ulid;

use crate::{CompactType, SqliteTask, config::Config, context::SqliteContext, from_row::TaskRow};

pub async fn fetch_next<Args, D: Codec<Args, Compact = CompactType>>(
    pool: SqlitePool,
    config: Config,
    worker: WorkerContext,
) -> Result<Vec<Task<Args, SqliteContext, Ulid>>, sqlx::Error>
where
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,

{
    let job_type = config.queue().to_string();
    let buffer_size = config.buffer_size() as i32;
    let worker = worker.name().to_string();
    sqlx::query_file_as!(
        TaskRow,
        "queries/backend/fetch_next.sql",
        worker,
        job_type,
        buffer_size
    )
    .fetch_all(&pool)
    .await?
    .into_iter()
    .map(|r| r.try_into_task::<D, Args>())
    .collect()
}

enum StreamState<Args> {
    Ready,
    Delay,
    Fetch(BoxFuture<'static, Result<Vec<SqliteTask<Args>>, sqlx::Error>>),
    Buffered(VecDeque<SqliteTask<Args>>),
    Empty,
}

/// Dispatcher for fetching tasks from a SQLite backend via [SqlitePollFetcher]
#[derive(Clone, Debug)]
pub struct SqliteFetcher<Args, Compact, Decode> {
    pub _marker: PhantomData<(Args, Compact, Decode)>,
}

/// Polling-based fetcher for retrieving tasks from a SQLite backend
#[pin_project]
pub struct SqlitePollFetcher<Args, Compact, Decode> {
    pool: SqlitePool,
    config: Config,
    wrk: WorkerContext,
    _marker: PhantomData<(Compact, Decode)>,
    #[pin]
    state: StreamState<Args>,

    #[pin]
    delay_stream: Option<Pin<Box<dyn Stream<Item = ()> + Send>>>,

    prev_count: Arc<AtomicUsize>,
}

impl<Args, Compact, Decode> Clone for SqlitePollFetcher<Args, Compact, Decode> {
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

impl<Args: 'static, Decode> SqlitePollFetcher<Args, CompactType, Decode> {
    pub fn new(pool: &Pool<Sqlite>, config: &Config, wrk: &WorkerContext) -> Self
    where
        Decode: Codec<Args, Compact = CompactType> + 'static,
        Decode::Error: std::error::Error + Send + Sync + 'static,
    {
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

impl<Args, Decode> Stream for SqlitePollFetcher<Args, CompactType, Decode>
where
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + 'static,
{
    type Item = Result<Option<SqliteTask<Args>>, sqlx::Error>;

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
                    let stream = fetch_next::<Args, Decode>(
                        this.pool.clone(),
                        this.config.clone(),
                        this.wrk.clone(),
                    );
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

impl<Args, Compact, Decode> SqlitePollFetcher<Args, Compact, Decode> {
    pub fn take_pending(&mut self) -> VecDeque<SqliteTask<Args>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}
