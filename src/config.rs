use std::time::Duration;

use apalis_core::backend::queue::Queue;
use serde::{Deserialize, Serialize};

/// Configuration for a worker's queue, batching, and liveness detection.
///
/// `Config` controls how jobs are fetched from a queue and how worker
/// liveness is monitored.
///
/// # Defaults
///
/// - `batch_size`: `10`
/// - `heartbeat_interval`: `30` seconds
/// - `missed_heartbeats`: `2`
/// - `queue`: `"default"`
/// - `database_url`: `None`
/// - `lock_tasks`: `true`
/// - `persist_results`: `true`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// The maximum number of jobs fetched in a single batch.
    ///
    /// Must be greater than zero.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// The interval between worker heartbeats.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: Duration,

    /// The number of missed heartbeats allowed before a worker is
    /// considered dead.
    #[serde(default = "default_missed_heartbeats")]
    pub missed_heartbeats: usize,

    /// The queue from which jobs are consumed.
    pub queue: Queue,

    /// An optional database URL used by the worker.
    pub database_url: Option<String>,

    /// Whether tasks should be locked while being processed.
    #[serde(default = "default_events")]
    pub lock_tasks: bool,

    /// Whether job results should be persisted.
    #[serde(default = "default_events")]
    pub persist_results: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            batch_size: 10,
            heartbeat_interval: Duration::from_secs(30),
            missed_heartbeats: 2,
            queue: Queue::from("default"),
            database_url: None,
            lock_tasks: true,
            persist_results: true,
        }
    }
}

fn default_batch_size() -> usize {
    10
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_missed_heartbeats() -> usize {
    2
}

fn default_events() -> bool {
    true
}

impl Config {
    /// Sets the maximum number of jobs to fetch in a single batch.
    ///
    /// Larger batches can improve throughput by reducing the number of
    /// queue operations, while smaller batches can reduce memory usage
    /// and improve job distribution between workers.
    ///
    /// # Panics
    ///
    /// Panics if `size` is `0`.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_sqlite::Config;
    ///
    /// let config = Config::default().batch_size(50);
    ///
    /// assert_eq!(config.batch_size, 50);
    /// ```
    #[must_use]
    pub fn batch_size(mut self, size: usize) -> Self {
        assert!(size > 0, "batch size cannot be 0");
        self.batch_size = size;
        self
    }

    /// Sets the interval between worker heartbeats.
    ///
    /// A shorter interval detects failed workers sooner but produces
    /// heartbeat activity more frequently.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use apalis_sqlite::Config;
    ///
    /// let config = Config::default()
    ///     .heartbeat_interval(Duration::from_secs(15));
    ///
    /// assert_eq!(config.heartbeat_interval, Duration::from_secs(15));
    /// ```
    #[must_use]
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Sets the queue from which jobs are consumed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use apalis_sqlite::Config;
    /// let config = Config::default()
    ///     .queue("high-priority");
    ///
    /// assert_eq!(config.queue.as_ref(), "high-priority");
    /// ```
    #[must_use]
    pub fn queue(mut self, queue: impl AsRef<str>) -> Self {
        self.queue = Queue::from(queue.as_ref());
        self
    }

    /// Sets the number of missed heartbeats allowed before a worker is
    /// considered dead.
    ///
    /// This value works together with [`Self::heartbeat_interval`].
    /// For example, a 30-second heartbeat interval with `2` missed
    /// heartbeats results in an orphan timeout of 60 seconds.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_sqlite::Config;
    ///
    /// let config = Config::default().missed_heartbeats(3);
    ///
    /// assert_eq!(config.missed_heartbeats, 3);
    /// assert_eq!(
    ///     config.orphaned_duration(),
    ///     std::time::Duration::from_secs(90)
    /// );
    /// ```
    #[must_use]
    pub fn missed_heartbeats(mut self, missed_heartbeats: usize) -> Self {
        self.missed_heartbeats = missed_heartbeats;
        self
    }

    /// Sets the database URL used by the worker.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_sqlite::Config;
    ///
    /// let config = Config::default()
    ///     .database_url(":memory:");
    ///
    /// assert_eq!(
    ///     config.database_url.as_deref(),
    ///     Some(":memory:")
    /// );
    /// ```
    #[must_use]
    pub fn database_url(mut self, database_url: impl Into<String>) -> Self {
        self.database_url = Some(database_url.into());
        self
    }

    /// Enables or disables task locking.
    ///
    /// When enabled, tasks are locked while being processed to prevent
    /// multiple workers from processing the same task concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_sqlite::Config;
    ///
    /// let config = Config::default().lock_tasks(false);
    ///
    /// assert!(!config.lock_tasks);
    /// ```
    #[must_use]
    pub fn lock_tasks(mut self, lock_tasks: bool) -> Self {
        self.lock_tasks = lock_tasks;
        self
    }

    /// Enables or disables result persistence.
    ///
    /// When enabled, results produced by completed jobs are persisted.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_sqlite::Config;
    ///
    /// let config = Config::default().persist_results(false);
    ///
    /// assert!(!config.persist_results);
    /// ```
    #[must_use]
    pub fn persist_results(mut self, persist_results: bool) -> Self {
        self.persist_results = persist_results;
        self
    }

    /// Returns the amount of time after which a worker may be considered
    /// orphaned.
    ///
    /// The duration is calculated as:
    ///
    /// ```text
    /// heartbeat_interval × missed_heartbeats
    /// ```
    #[must_use]
    pub fn orphaned_duration(&self) -> Duration {
        self.heartbeat_interval * self.missed_heartbeats as u32
    }
}
