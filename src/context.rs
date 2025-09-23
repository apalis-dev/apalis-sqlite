use std::convert::Infallible;

use apalis_core::task_fn::FromRequest;

use serde::{Deserialize, Serialize};

use crate::SqliteTask;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteContext {
    max_attempts: i32,
    last_result: Option<String>,
    lock_at: Option<i64>,
    lock_by: Option<String>,
    done_at: Option<i64>,
    priority: i32,
    namespace: Option<String>,
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
            namespace: None,
        }
    }

    /// Set the number of attempts
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
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
    pub fn with_done_at(mut self, done_at: Option<i64>) -> Self {
        self.done_at = done_at;
        self
    }

    /// Get the time a job was locked
    pub fn lock_at(&self) -> &Option<i64> {
        &self.lock_at
    }

    /// Set the lock_at value
    pub fn with_lock_at(mut self, lock_at: Option<i64>) -> Self {
        self.lock_at = lock_at;
        self
    }

    /// Get the time a job was locked
    pub fn lock_by(&self) -> &Option<String> {
        &self.lock_by
    }

    /// Set `lock_by`
    pub fn with_lock_by(mut self, lock_by: Option<String>) -> Self {
        self.lock_by = lock_by;
        self
    }

    /// Get the time a job was locked
    pub fn last_result(&self) -> &Option<String> {
        &self.last_result
    }

    /// Set the last result
    pub fn with_last_result(mut self, result: Option<String>) -> Self {
        self.last_result = result;
        self
    }

    /// Set the job priority. Larger values will run sooner. Default is 0.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Get the job priority
    pub fn priority(&self) -> i32 {
        self.priority
    }

    pub fn namespace(&self) -> &Option<String> {
        &self.namespace
    }

    pub fn with_namespace(mut self, namespace: String) -> Self {
        self.namespace = Some(namespace);
        self
    }
}

impl<Args: Sync> FromRequest<SqliteTask<Args>> for SqliteContext {
    type Error = Infallible;
    async fn from_request(req: &SqliteTask<Args>) -> Result<Self, Self::Error> {
        Ok(req.parts.ctx.clone())
    }
}
