use std::time::Duration;

use apalis::prelude::*;
use apalis_sqlite::*;
use apalis_workflow::*;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderJob {
    order_id: i64,
}

#[derive(Debug, Clone, FromRow)]
struct OrderRecord {
    id: i64,
    customer_email: String,
    total_cents: i64,
    payment_txn: Option<String>,
    tracking_number: Option<String>,
    status: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
struct OrderItemRecord {
    id: i64,
    order_id: i64,
    sku: String,
    in_stock: bool,
}

#[derive(Clone)]
struct PaymentService {
    // e.g. stripe::Client
}

impl PaymentService {
    async fn charge(&self, order_id: i64, amount_cents: i64) -> Result<String, BoxDynError> {
        // Call your payment gateway here and return the transaction ID.
        Ok(format!("charge order {order_id} for {amount_cents} cents"))
    }

    async fn refund(&self, txn_id: &str) -> Result<(), BoxDynError> {
        // Refund a previously captured transaction.
        todo!("refund transaction {txn_id}")
    }
}

#[derive(Clone)]
struct InventoryService {
    // e.g. reqwest::Client + base_url
}

impl InventoryService {
    async fn is_available(&self, sku: &str) -> Result<bool, BoxDynError> {
        println!("Checking stock for {sku}");
        Ok(true)
    }
}

#[derive(Clone)]
struct ShipmentService {
    // e.g. fedex_sdk::Client
}

impl ShipmentService {
    async fn dispatch(&self, order_id: i64, item_count: i64) -> Result<String, BoxDynError> {
        // Submit shipment and return a tracking number.
        Ok(format!("dispatch {item_count} items for order {order_id}"))
    }
}

#[derive(Clone)]
struct EmailService {
    // e.g. sendgrid::SGClient
}

impl EmailService {
    async fn send_shipment_confirmation(
        &self,
        to: &str,
        order_id: i64,
        tracking_number: &str,
    ) -> Result<(), BoxDynError> {
        // Send a transactional email via your provider.
        println!("email {to} about order {order_id} with tracking {tracking_number}");
        Ok(())
    }
}

async fn charge_payment(
    job: OrderJob,
    db: Data<SqlitePool>,
    payment: Data<PaymentService>,
) -> Result<Vec<OrderItemRecord>, BoxDynError> {
    let order: OrderRecord = sqlx::query_as("SELECT * FROM orders WHERE id = ?")
        .bind(job.order_id)
        .fetch_one(&*db)
        .await?;

    if order.payment_txn.is_some() {
        // already charged on a previous attempt
        return Err("Order is already processed".into());
    }

    let txn = payment.charge(order.id, order.total_cents).await?;

    sqlx::query("UPDATE orders SET payment_txn = ?, status = 'charged' WHERE id = ?")
        .bind(&txn)
        .bind(order.id)
        .execute(&*db)
        .await?;

    let items: Vec<OrderItemRecord> =
        sqlx::query_as("SELECT * FROM order_items WHERE order_id = ?")
            .bind(job.order_id)
            .fetch_all(&*db)
            .await?;

    Ok(items)
}

async fn filter_unavailable(
    item: OrderItemRecord,
    inventory: Data<InventoryService>,
) -> Result<Option<OrderItemRecord>, BoxDynError> {
    let available = inventory.is_available(&item.sku).await?;
    if available { Ok(Some(item)) } else { Ok(None) }
}

async fn refund_if_all_unavailable(
    items: Vec<OrderItemRecord>,
    db: Data<SqlitePool>,
    payment: Data<PaymentService>,
) -> Result<OrderJob, BoxDynError> {
    let order_id = items[0].order_id;
    let order: OrderRecord = sqlx::query_as("SELECT * FROM orders WHERE id = ?")
        .bind(order_id)
        .fetch_one(&*db)
        .await?;

    let available_count = items.len();

    for item in &items {
        let OrderItemRecord { id, .. } = item;
        sqlx::query("UPDATE order_items SET in_stock = ? WHERE id = ?")
            .bind(true)
            .bind(id)
            .execute(&*db)
            .await?;
    }

    if available_count == 0 {
        if let Some(txn) = &order.payment_txn {
            payment.refund(txn).await?;
            sqlx::query("UPDATE orders SET status = 'refunded' WHERE id = ?")
                .bind(order.id)
                .execute(&*db)
                .await?;
        }

        return Err(format!("No items in stock for order {}", order.id).into());
    }

    sqlx::query("UPDATE orders SET status = 'stock_confirmed' WHERE id = ?")
        .bind(order.id)
        .execute(&*db)
        .await?;

    Ok(OrderJob { order_id: order_id })
}

async fn dispatch_shipment(
    job: OrderJob,
    db: Data<SqlitePool>,
    shipment: Data<ShipmentService>,
) -> Result<OrderJob, BoxDynError> {
    let order: OrderRecord = sqlx::query_as("SELECT * FROM orders WHERE id = ?")
        .bind(job.order_id)
        .fetch_one(&*db)
        .await?;

    if order.tracking_number.is_some() {
        return Ok(job); // already dispatched on a previous attempt
    }

    let item_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM order_items WHERE order_id = ? AND in_stock = TRUE",
    )
    .bind(job.order_id)
    .fetch_one(&*db)
    .await?;

    let tracking = shipment.dispatch(order.id, item_count).await?;

    sqlx::query("UPDATE orders SET tracking_number = ?, status = 'dispatched' WHERE id = ?")
        .bind(&tracking)
        .bind(order.id)
        .execute(&*db)
        .await?;

    Ok(job)
}

async fn notify_customer(
    job: OrderJob,
    db: Data<SqlitePool>,
    email: Data<EmailService>,
) -> Result<(), BoxDynError> {
    let order: OrderRecord = sqlx::query_as("SELECT * FROM orders WHERE id = ?")
        .bind(job.order_id)
        .fetch_one(&*db)
        .await?;

    if order.status == "completed" {
        return Ok(()); // notification already sent on a previous attempt
    }

    let tracking = order.tracking_number.as_deref().unwrap_or_default();

    email
        .send_shipment_confirmation(&order.customer_email, order.id, tracking)
        .await?;

    sqlx::query("UPDATE orders SET status = 'completed' WHERE id = ?")
        .bind(order.id)
        .execute(&*db)
        .await?;

    Ok(())
}

async fn setup_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS orders (
            id              INTEGER PRIMARY KEY,
            customer_email  TEXT    NOT NULL,
            total_cents     INTEGER NOT NULL,
            payment_txn     TEXT,
            tracking_number TEXT,
            status          TEXT    NOT NULL
        );",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS order_items (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            sku      TEXT    NOT NULL,
            in_stock BOOLEAN DEFAULT FALSE,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        );",
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn seed_order(
    pool: &SqlitePool,
    order_id: i64,
    email: &str,
    total_cents: i64,
    skus: &[&str],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO orders (id, customer_email, total_cents, status) VALUES (?, ?, ?, 'pending')",
    )
    .bind(order_id)
    .bind(email)
    .bind(total_cents)
    .execute(pool)
    .await?;

    for sku in skus {
        sqlx::query("INSERT INTO order_items (order_id, sku) VALUES (?, ?)")
            .bind(order_id)
            .bind(sku)
            .execute(pool)
            .await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();

    setup_schema(&pool).await.unwrap();
    SqliteStorage::setup(&pool).await.unwrap();

    seed_order(
        &pool,
        1001,
        "alice@example.com",
        7499,
        &["WIDGET-A", "WIDGET-B", "WIDGET-C"],
    )
    .await
    .unwrap();

    let mut sqlite = SqliteStorage::new_in_queue(&pool, "order-fulfilment");
    sqlite
        .push_start(OrderJob { order_id: 1001 })
        .await
        .unwrap();

    let workflow = Workflow::new("order-fulfilment")
        .and_then(charge_payment)
        .filter_map(filter_unavailable)
        .and_then(refund_if_all_unavailable)
        .delay_for(Duration::from_millis(500))
        .and_then(dispatch_shipment)
        .and_then(notify_customer);

    let worker = WorkerBuilder::new("fulfilment-worker")
        .backend(sqlite)
        .data(pool)
        .data(PaymentService { /* ... */ })
        .data(InventoryService { /* ... */ })
        .data(ShipmentService { /* ... */ })
        .data(EmailService { /* ... */ })
        .on_event(|_ctx, ev| println!("Event: {:?}", ev))
        .build(workflow);

    worker.run().await.unwrap();
}
