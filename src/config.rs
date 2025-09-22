use std::time::Duration;

use apalis_core::backend::poll_strategy::{IntervalStrategy, MultiStrategy, StrategyBuilder};
use sqlx::sqlite::SqliteConnectOptions;

#[derive(Debug, Clone)]
pub struct Config {
    keep_alive: Duration,
    buffer_size: usize,
    poll_strategy: MultiStrategy,
    reenqueue_orphaned_after: Duration,
    namespace: String,
    ack: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            keep_alive: Duration::from_secs(30),
            buffer_size: 10,
            poll_strategy: StrategyBuilder::new()
                .apply(IntervalStrategy::new(Duration::from_millis(100)))
                .build(),
            reenqueue_orphaned_after: Duration::from_secs(300), // 5 minutes
            namespace: String::from("apalis::sqlite"),
            ack: true,
        }
    }
}

impl Config {
    /// Create a new config with a jobs namespace
    pub fn new(namespace: &str) -> Self {
        Config::default().set_namespace(namespace)
    }

    /// Interval between database poll queries
    ///
    /// Defaults to 100ms
    pub fn with_poll_interval(mut self, strategy: MultiStrategy) -> Self {
        self.poll_strategy = strategy;
        self
    }

    /// Interval between worker keep-alive database updates
    ///
    /// Defaults to 30s
    pub fn set_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Buffer size to use when querying for jobs
    ///
    /// Defaults to 10
    pub fn set_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the namespace to consume and push jobs to
    ///
    /// Defaults to "apalis::sql"
    pub fn set_namespace(mut self, namespace: &str) -> Self {
        self.namespace = namespace.to_string();
        self
    }

    /// Gets a reference to the keep_alive duration.
    pub fn keep_alive(&self) -> &Duration {
        &self.keep_alive
    }

    /// Gets a mutable reference to the keep_alive duration.
    pub fn keep_alive_mut(&mut self) -> &mut Duration {
        &mut self.keep_alive
    }

    /// Gets the buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Gets a reference to the poll_strategy.
    pub fn poll_strategy(&self) -> &MultiStrategy {
        &self.poll_strategy
    }

    /// Gets a mutable reference to the poll_strategy.
    pub fn poll_strategy_mut(&mut self) -> &mut MultiStrategy {
        &mut self.poll_strategy
    }

    /// Gets a reference to the namespace.
    pub fn namespace(&self) -> &String {
        &self.namespace
    }

    /// Gets a mutable reference to the namespace.
    pub fn namespace_mut(&mut self) -> &mut String {
        &mut self.namespace
    }

    /// Gets the reenqueue_orphaned_after duration.
    pub fn reenqueue_orphaned_after(&self) -> Duration {
        self.reenqueue_orphaned_after
    }

    /// Gets a mutable reference to the reenqueue_orphaned_after.
    pub fn reenqueue_orphaned_after_mut(&mut self) -> &mut Duration {
        &mut self.reenqueue_orphaned_after
    }

    /// Occasionally some workers die, or abandon jobs because of panics.
    /// This is the time a task takes before its back to the queue
    ///
    /// Defaults to 5 minutes
    pub fn set_reenqueue_orphaned_after(mut self, after: Duration) -> Self {
        self.reenqueue_orphaned_after = after;
        self
    }

    pub fn ack(&self) -> bool {
        self.ack
    }

    pub fn set_ack(mut self, auto_ack: bool) -> Self {
        self.ack = auto_ack;
        self
    }
}
