use apalis_core::{
    error::{AbortError, BoxDynError},
    task::{Parts, status::Status},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use futures::{FutureExt, future::BoxFuture};
use serde::Serialize;
use sqlx::SqlitePool;
use tower_layer::Layer;
use tower_service::Service;
use ulid::Ulid;

use crate::{SqliteTask, context::SqliteContext};

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
        let status = calculate_status(parts, res);
        parts.status.store(status.clone());
        let attempt = parts.attempt.current() as i32;
        let pool = self.pool.clone();
        let res = response.map_err(|e| sqlx::Error::Decode(e.into()));
        let status = status.to_string();
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

pub async fn lock_task(
    pool: &SqlitePool,
    task_id: &Ulid,
    worker_id: &str,
) -> Result<(), sqlx::Error> {
    let task_id = task_id.to_string();
    let res = sqlx::query_file!("queries/task/lock.sql", task_id, worker_id)
        .execute(pool)
        .await?;

    if res.rows_affected() == 0 {
        return Err(sqlx::Error::RowNotFound);
    }
    Ok(())
}

pub struct LockTaskLayer {
    pool: SqlitePool,
}

impl LockTaskLayer {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for LockTaskLayer {
    type Service = LockTaskService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LockTaskService {
            inner,
            pool: self.pool.clone(),
        }
    }
}

pub struct LockTaskService<S> {
    inner: S,
    pool: SqlitePool,
}

impl<S, Args> Service<SqliteTask<Args>> for LockTaskService<S>
where
    S: Service<SqliteTask<Args>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    Args: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: SqliteTask<Args>) -> Self::Future {
        let pool = self.pool.clone();
        let worker_id = req
            .parts
            .data
            .get::<WorkerContext>()
            .map(|w| w.name().to_owned())
            .unwrap();
        let parts = &req.parts;
        let task_id = match &parts.task_id {
            Some(id) => id.inner().clone(),
            None => {
                return async {
                    Err(sqlx::Error::ColumnNotFound("TASK_ID_FOR_LOCK".to_owned()).into())
                }
                .boxed();
            }
        };
        let fut = self.inner.call(req);
        async move {
            lock_task(&pool, &task_id, &worker_id).await?;
            fut.await.map_err(|e| e.into())
        }
        .boxed()
    }
}
