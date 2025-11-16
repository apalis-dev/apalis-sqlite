//! Module for various query implementations for the SQLite backend.
//!
//! Each submodule contains specific query logic used by the backend.
use apalis_core::backend::StatType;
/// Fetch tasks by their IDs
pub mod fetch_by_id;
/// Keep workers alive by updating their heartbeat
pub mod keep_alive;
/// List available queues
pub mod list_queues;
/// List tasks in a specific queue
pub mod list_tasks;
/// List workers
pub mod list_workers;
/// Metrics related queries
pub mod metrics;
/// Re-enqueue orphaned tasks that were being processed by dead workers
pub mod reenqueue_orphaned;
/// Register a new worker in the database
pub mod register_worker;
/// Vacuum the database to optimize space
pub mod vacuum;
/// Wait for tasks to complete and stream their results
pub mod wait_for;

/// Convert a string representation of a stat type to the corresponding `StatType` enum variant
fn stat_type_from_string(s: &str) -> StatType {
    match s {
        "Decimal" => StatType::Decimal,
        "Percentage" => StatType::Percentage,
        "Timestamp" => StatType::Timestamp,
        _ => StatType::Number, // default fallback
    }
}
