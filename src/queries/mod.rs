use apalis_core::backend::StatType;

pub mod fetch_by_id;
pub mod keep_alive;
pub mod list_queues;
pub mod list_tasks;
pub mod list_workers;
pub mod metrics;
pub mod reenqueue_orphaned;
pub mod register_worker;
pub mod vacuum;
pub mod wait_for;

fn stat_type_from_string(s: &str) -> StatType {
    match s {
        "Number" => StatType::Number,
        "Decimal" => StatType::Decimal,
        "Percentage" => StatType::Percentage,
        "Timestamp" => StatType::Timestamp,
        _ => StatType::Number, // default fallback
    }
}
