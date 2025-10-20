use std::time::Duration;

use futures::{FutureExt, Stream, stream};
use sqlx::SqlitePool;

use crate::Config;

pub fn reenqueue_orphaned(
    pool: SqlitePool,
    config: Config,
) -> impl Future<Output = Result<u64, sqlx::Error>> + Send {
    let dead_for = config.reenqueue_orphaned_after().as_secs() as i64;
    let queue = config.queue().to_string();
    async move {
        match sqlx::query_file!("queries/backend/reenqueue_orphaned.sql", dead_for, queue,)
            .execute(&pool)
            .await
        {
            Ok(res) => {
                if res.rows_affected() > 0 {
                    log::info!(
                        "Re-enqueued {} orphaned tasks that were being processed by dead workers",
                        res.rows_affected()
                    );
                }
                Ok(res.rows_affected())
            }
            Err(e) => {
                log::error!("Failed to re-enqueue orphaned tasks: {e}");
                Err(e)
            }
        }
    }
}

pub fn reenqueue_orphaned_stream(
    pool: SqlitePool,
    config: Config,
    interval: Duration,
) -> impl Stream<Item = Result<u64, sqlx::Error>> + Send {
    let config = config.clone();
    stream::unfold((), move |_| {
        let pool = pool.clone();
        let config = config.clone();
        let interval = apalis_core::timer::Delay::new(interval);
        let fut = async move {
            interval.await;
            reenqueue_orphaned(pool, config).await
        };
        fut.map(|res| Some((res, ())))
    })
}
