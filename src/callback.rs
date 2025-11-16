use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::{Stream, StreamExt};
use sqlx::sqlite::{SqliteOperation, UpdateHookResult};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

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
    tx.start_send(DbEvent {
        op: event.operation,
        db_name: event.database.to_owned(),
        table_name: event.table.to_owned(),
        rowid: event.rowid,
    })
    .unwrap();
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
    type Item = DbEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.lock().unwrap().poll_next_unpin(cx)
    }
}
