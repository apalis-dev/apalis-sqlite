use apalis_core::{
    error::{AbortError, BoxDynError},
    task::{Parts, status::Status},
    worker::ext::ack::Acknowledge,
};
use futures::{FutureExt, future::BoxFuture};
use serde::Serialize;
use sqlx::SqlitePool;
use ulid::Ulid;

use crate::context::SqliteContext;

#[derive(Clone)]
pub struct SqliteAck {
    pool: SqlitePool,
}
impl SqliteAck {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

impl<Res: Serialize> Acknowledge<Res, SqliteContext, Ulid> for SqliteAck {
    type Error = sqlx::Error;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;
    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<SqliteContext, Ulid>,
    ) -> Self::Future {
        let task_id = parts.task_id;
        let worker_id = parts.ctx.lock_by().clone();

        let response = serde_json::to_string(&res.as_ref().map_err(|e| e.to_string()));
        let status = calculate_status(parts, res).to_string();
        let attempt = parts.attempt.current() as i32;
        let pool = self.pool.clone();
        let res = response.map_err(|e| sqlx::Error::Decode(e.into()));

        async move {
            let task_id = task_id
                .ok_or(sqlx::Error::ColumnNotFound("TASK_ID_FOR_ACK".to_owned()))?
                .to_string();
            let worker_id =
                worker_id.ok_or(sqlx::Error::ColumnNotFound("WORKER_ID_LOCK_BY".to_owned()))?;
            let res_ok = res?;
            let res = sqlx::query_file!(
                "queries/task/ack.sql",
                task_id,
                attempt,
                res_ok,
                status,
                worker_id
            )
            .execute(&pool)
            .await?;

            if res.rows_affected() == 0 {
                return Err(sqlx::Error::RowNotFound);
            }
            Ok(())
        }
        .boxed()
    }
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
