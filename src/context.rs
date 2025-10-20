use std::{collections::HashMap, convert::Infallible};

use apalis_core::{task::metadata::MetadataExt, task_fn::FromRequest};

use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, Error},
};

use crate::{CompactType, SqliteTask};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteContext {
    max_attempts: i32,
    last_result: Option<String>,
    lock_at: Option<i64>,
    lock_by: Option<String>,
    done_at: Option<i64>,
    priority: i32,
    queue: Option<String>,
    meta: HashMap<String, CompactType>,
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
            queue: None,
            meta: HashMap::new(),
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

    pub fn queue(&self) -> &Option<String> {
        &self.queue
    }

    pub fn with_queue(mut self, queue: String) -> Self {
        self.queue = Some(queue);
        self
    }

    pub fn meta(&self) -> &HashMap<String, CompactType> {
        &self.meta
    }

    pub fn with_meta(mut self, meta: HashMap<String, CompactType>) -> Self {
        self.meta = meta;
        self
    }
}

impl<Args: Sync> FromRequest<SqliteTask<Args>> for SqliteContext {
    type Error = Infallible;
    async fn from_request(req: &SqliteTask<Args>) -> Result<Self, Self::Error> {
        Ok(req.parts.ctx.clone())
    }
}

impl<T: DeserializeOwned + Serialize> MetadataExt<T> for SqliteContext {
    type Error = serde_json::Error;
    fn extract(&self) -> Result<T, Self::Error> {
        self.meta
            .get(&std::any::type_name::<T>().to_string())
            .and_then(|v| serde_json::from_str::<T>(v).ok())
            .ok_or(serde_json::Error::custom("Failed to extract metadata"))
    }
    fn inject(&mut self, value: T) -> Result<(), Self::Error> {
        self.meta.insert(
            std::any::type_name::<T>().to_string(),
            serde_json::to_string(&value).unwrap(),
        );
        Ok(())
    }
}
