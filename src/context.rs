use std::convert::Infallible;

use apalis_core::{
    task::{Task, status::Status},
    task_fn::FromRequest,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::SqliteTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteContext {
    max_attempts: i32,
    last_result: Option<String>,
    lock_at: Option<i64>,
    lock_by: Option<String>,
    done_at: Option<i64>,
    priority: i32,
}

impl Default for SqliteContext {
    fn default() -> Self {
        Self::new()
    }
}

impl SqliteContext {
    /// Build a new context with defaults
    pub fn new() -> Self {
        SqliteContext {
            lock_at: None,
            done_at: None,
            max_attempts: 5,
            last_result: None,
            lock_by: None,
            priority: 0,
        }
    }

    /// Set the number of attempts
    pub fn set_max_attempts(&mut self, max_attempts: i32) {
        self.max_attempts = max_attempts;
    }

    /// Gets the maximum attempts for a job. Default 25
    pub fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    /// Get the time a job was done
    pub fn done_at(&self) -> &Option<i64> {
        &self.done_at
    }

    /// Set the time a job was done
    pub fn set_done_at(&mut self, done_at: Option<i64>) {
        self.done_at = done_at;
    }

    /// Get the time a job was locked
    pub fn lock_at(&self) -> &Option<i64> {
        &self.lock_at
    }

    /// Set the lock_at value
    pub fn set_lock_at(&mut self, lock_at: Option<i64>) {
        self.lock_at = lock_at;
    }

    /// Get the time a job was locked
    pub fn lock_by(&self) -> &Option<String> {
        &self.lock_by
    }

    /// Set `lock_by`
    pub fn set_lock_by(&mut self, lock_by: Option<String>) {
        self.lock_by = lock_by;
    }

    /// Get the time a job was locked
    pub fn last_result(&self) -> &Option<String> {
        &self.last_result
    }

    /// Set the last result
    pub fn set_last_result(&mut self, result: Option<String>) {
        self.last_result = result;
    }

    /// Set the job priority. Larger values will run sooner. Default is 0.
    pub fn set_priority(&mut self, priority: i32) {
        self.priority = priority
    }

    /// Get the job priority
    pub fn priority(&self) -> i32 {
        self.priority
    }
}

impl<Args: Sync> FromRequest<SqliteTask<Args>> for SqliteContext {
    type Error = Infallible;
    async fn from_request(req: &SqliteTask<Args>) -> Result<Self, Self::Error> {
        Ok(req.parts.ctx.clone())
    }
}
