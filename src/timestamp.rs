use apalis_sql::SqlTimestamp;

// ============================================================================
// Chrono implementation
// ============================================================================

#[cfg(all(feature = "chrono", not(feature = "time")))]
mod inner {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};

    /// Public newtype wrapper - implements SqlTimestamp for TaskRow<SqliteDateTime>
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct SqliteDateTime(pub DateTime<Utc>);

    impl SqlTimestamp for SqliteDateTime {
        fn to_unix_timestamp(&self) -> i64 {
            self.0.timestamp()
        }
    }

    impl SqliteDateTime {
        /// Create from unix timestamp
        pub fn from_unix_timestamp(ts: i64) -> Option<Self> {
            Utc.timestamp_opt(ts, 0).single().map(Self)
        }
    }
}

// ============================================================================
// Time implementation
// ============================================================================

#[cfg(feature = "time")]
mod inner {
    use super::*;
    use time::OffsetDateTime;

    /// Public newtype wrapper - implements SqlTimestamp for TaskRow<SqliteDateTime>
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct SqliteDateTime(pub OffsetDateTime);

    impl SqlTimestamp for SqliteDateTime {
        fn to_unix_timestamp(&self) -> i64 {
            self.0.unix_timestamp()
        }
    }

    impl SqliteDateTime {
        /// Create from unix timestamp
        pub fn from_unix_timestamp(ts: i64) -> Option<Self> {
            OffsetDateTime::from_unix_timestamp(ts).ok().map(Self)
        }
    }
}

// ============================================================================
// Re-exports
// ============================================================================

#[cfg(any(feature = "chrono", feature = "time"))]
pub use inner::SqliteDateTime;

#[cfg(not(any(feature = "chrono", feature = "time")))]
compile_error!("Either 'chrono' or 'time' feature must be enabled");
