//! Callbacks allow realtime listeners for new jobs
//!
//! ## Example usage
//!
//! ```ignore
//! let (pool, callback) = SqliteStorage::connect_with_callback(url).unwrap();
//! SqliteStorage::setup(&pool).await.unwrap();
//! let backend = SqliteStorage::new(&pool).with_callback(callback);
//! ```
use apalis_core::backend::ext::poll_strategy::{PollWith, StreamStrategy};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::{Stream, StreamExt};
use sqlx::sqlite::{SqliteOperation, UpdateHookResult};

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::{JOBS_TABLE, SqliteStorage};

/// An [SqliteStorage] that polls when [HookCallbackListener] is invoked
pub type SqliteStorageWithHook<Args> =
    PollWith<SqliteStorage<Args>, StreamStrategy<HookCallbackListener>>;

/// Database event emitted by SQLite update hook
#[derive(Debug)]
pub struct DbEvent {
    op: SqliteOperation,
    db_name: String,
    table_name: String,
    rowid: i64,
}

impl DbEvent {
    /// Get the operation type of the database event
    #[must_use]
    pub fn operation(&self) -> &SqliteOperation {
        &self.op
    }

    /// Get the database name of the database event
    #[must_use]
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    /// Get the table name of the database event
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get the rowid of the database event
    #[must_use]
    pub fn rowid(&self) -> i64 {
        self.rowid
    }
}

// Callback for SQLite update hook
pub(crate) fn update_hook_callback(event: UpdateHookResult<'_>, tx: &mut UnboundedSender<DbEvent>) {
    if event.operation == SqliteOperation::Insert && event.table == JOBS_TABLE {
        let _ = tx.start_send(DbEvent {
            op: event.operation,
            db_name: event.database.to_owned(),
            table_name: event.table.to_owned(),
            rowid: event.rowid,
        });
    }
}

/// Listener for database events emitted by SQLite update hook
#[derive(Debug, Clone)]
pub struct HookCallbackListener {
    rx: Arc<Mutex<UnboundedReceiver<DbEvent>>>,
}

impl HookCallbackListener {
    /// Create a new HookCallbackListener
    #[must_use]
    pub fn new(rx: UnboundedReceiver<DbEvent>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

impl Stream for HookCallbackListener {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        log::trace!("HookCallbackListener: poll_next");
        match self.rx.lock().unwrap().poll_next_unpin(cx) {
            Poll::Ready(Some(_)) => Poll::Ready(Some(())),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
        .map(|s| {
            log::trace!(
                "HookCallbackListener: poll_ready: {ready}",
                ready = s.is_some()
            );
            s
        })
    }
}
