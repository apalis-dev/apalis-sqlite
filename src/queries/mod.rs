//! Queries needed for polling, updating and exposing tasks.
// /// ACK a task on success or failure
mod ack_task;

/// Fetch tasks by their IDs
mod fetch_by_id;

/// Fetch the next stream of tasks
mod fetch_next;
/// Keep workers alive by updating their heartbeat
mod keep_alive;
/// List available queues
mod list_queues;
/// List tasks in a specific queue
mod list_tasks;
/// List workers
mod list_workers;
/// Lock a task for processing
mod lock_task;
/// Metrics related queries
mod metrics;
/// Re-enqueue orphaned tasks that were being processed by dead workers
mod reenqueue_orphaned;
/// Register a new worker in the database
mod register_worker;
/// Vacuum the database to optimize space
mod vacuum;
/// Wait for tasks to complete and stream their results
mod wait_for;

pub use crate::queries::{
    ack_task::AckPayload, ack_task::ack_tasks, fetch_next::fetch_next, keep_alive::keep_alive,
    lock_task::lock_tasks, reenqueue_orphaned::reenqueue_abandoned,
    reenqueue_orphaned::reenqueue_orphaned, register_worker::register_worker,
};
pub use crate::sink::push_tasks;
