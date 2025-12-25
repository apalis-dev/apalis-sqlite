use apalis::prelude::*;
use apalis_sqlite::SqliteStorage;
use apalis_workflow::*;
use sqlx::SqlitePool;

async fn get_name(user_id: u32) -> Result<String, BoxDynError> {
    Ok(user_id.to_string())
}

async fn get_age(user_id: u32) -> Result<usize, BoxDynError> {
    Ok(user_id as usize + 20)
}

async fn get_address(user_id: u32) -> Result<usize, BoxDynError> {
    Ok(user_id as usize + 100)
}

async fn collector(
    (name, age, address): (String, usize, usize),
    wrk: WorkerContext,
) -> Result<usize, BoxDynError> {
    let result = name.parse::<usize>()? + age + address;
    wrk.stop().unwrap();
    Ok(result)
}

#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();
    let mut backend = SqliteStorage::new(&pool);
    backend.push_start(vec![42, 43, 44]).await.unwrap();

    let dag_flow = DagFlow::new("user-etl-workflow");
    let get_name = dag_flow.node(get_name);
    let get_age = dag_flow.node(get_age);
    let get_address = dag_flow.node(get_address);
    dag_flow
        .node(collector)
        .depends_on((&get_name, &get_age, &get_address)); // Order and types matters here

    dag_flow.validate().unwrap(); // Ensure DAG is valid

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|_c, e| {
            println!("{e:?},");
        })
        .build(dag_flow);
    worker.run().await.unwrap();
}
