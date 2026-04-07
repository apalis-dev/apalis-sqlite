use apalis_core::{
    error::{AbortError, BoxDynError},
    task::{Parts, status::Status},
};

use sqlx::{Executor, Sqlite};
use ulid::Ulid;

use crate::SqliteContext;

/// Lock a task, given a worker
pub async fn ack_task<E: for<'a> Executor<'a, Database = Sqlite>>(
    pool: E,
    task_id: &str,
    worker_id: &str,
    res: &str,
    status: &Status,
    attempt: i32,
) -> Result<(), sqlx::Error> {
    let status = status.to_string();
    let res = sqlx::query_file!(
        "queries/task/ack.sql",
        task_id,
        attempt,
        res,
        status,
        worker_id
    )
    .execute(pool)
    .await?;

    if res.rows_affected() == 0 {
        return Err(sqlx::Error::RowNotFound);
    }
    Ok(())
}

pub fn calculate_status<Res>(
    parts: &Parts<SqliteContext, Ulid>,
    res: &Result<Res, BoxDynError>,
) -> Status {
    match &res {
        Ok(_) => Status::Done,
        Err(e) => match e {
            _ if parts.ctx.max_attempts() as usize <= parts.attempt.current() => Status::Killed,
            e if e.downcast_ref::<AbortError>().is_some() => Status::Killed,
            _ => Status::Failed,
        },
    }
}
