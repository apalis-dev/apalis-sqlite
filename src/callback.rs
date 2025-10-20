use futures::channel::mpsc::{self, UnboundedReceiver};
use futures::{Stream, StreamExt};
use std::ffi::{CStr, c_void};
use std::os::raw::{c_char, c_int};
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct DbEvent {
    op: &'static str,
    db_name: String,
    table_name: String,
    rowid: i64,
}

impl DbEvent {
    pub fn operation(&self) -> &'static str {
        self.op
    }

    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn rowid(&self) -> i64 {
        self.rowid
    }
}

// Callback for SQLite update hook
pub(crate) extern "C" fn update_hook_callback(
    arg: *mut c_void,
    op: c_int,
    db_name: *const c_char,
    table_name: *const c_char,
    rowid: i64,
) {
    let op_str = match op {
        libsqlite3_sys::SQLITE_INSERT => "INSERT",
        libsqlite3_sys::SQLITE_UPDATE => "UPDATE",
        libsqlite3_sys::SQLITE_DELETE => "DELETE",
        _ => "UNKNOWN",
    };

    unsafe {
        let db = CStr::from_ptr(db_name).to_string_lossy().to_string();
        let table = CStr::from_ptr(table_name).to_string_lossy().to_string();

        // Recover sender from raw pointer
        let tx = &mut *(arg as *mut mpsc::UnboundedSender<DbEvent>);

        // Ignore send errors (receiver closed)
        let _ = tx.start_send(DbEvent {
            op: op_str,
            db_name: db,
            table_name: table,
            rowid,
        });
    }
}

#[derive(Debug, Clone)]
pub struct HookCallbackListener;

#[derive(Debug)]
pub struct CallbackListener {
    rx: UnboundedReceiver<DbEvent>,
}
impl CallbackListener {
    pub fn new(rx: UnboundedReceiver<DbEvent>) -> Self {
        Self { rx }
    }
}

impl Stream for CallbackListener {
    type Item = DbEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_next_unpin(cx)
    }
}
