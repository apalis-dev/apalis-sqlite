use std::{collections::HashMap, time::Duration};

use apalis::prelude::*;
use apalis_sqlite::{SqliteStorage, shared::SqliteStorageFactory};

use futures::stream;

#[tokio::main]
async fn main() {
    let mut store = SqliteStorageFactory::new(":memory:");

    SqliteStorage::setup(store.pool()).await.unwrap();

    let mut map_store = store.create().unwrap();

    let mut int_store = store.create().unwrap();

    map_store
        .push_stream(&mut stream::iter(vec![HashMap::<String, String>::new()]))
        .await
        .unwrap();
    int_store.push(99).await.unwrap();

    async fn send_reminder<T>(
        _: T,
        _task_id: TaskId,
        wrk: WorkerContext,
    ) -> Result<(), BoxDynError> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        wrk.stop().unwrap();
        println!("Reminder sent!");
        Ok(())
    }

    let int_worker = WorkerBuilder::new("rango-tango-2")
        .backend(int_store)
        .build(send_reminder);
    let map_worker = WorkerBuilder::new("rango-tango-1")
        .backend(map_store)
        .build(send_reminder);
    tokio::try_join!(int_worker.run(), map_worker.run()).unwrap();
}
