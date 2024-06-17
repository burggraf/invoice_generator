use sqlx::postgres::PgPoolOptions;
use fake::{Fake};
use fake::faker::name::en::*;
use rand::Rng;
use rand::prelude::SliceRandom;
use chrono::{NaiveDate, NaiveDateTime, Duration};
use bigdecimal::BigDecimal;
use std::time::Instant;
use std::str::FromStr;
use dotenv::dotenv;
use std::env;

#[derive(Debug)]
struct Invoice {
    customer_id: i32,
    customer_name: String,
    invoice_date: NaiveDateTime,
    due_date: NaiveDateTime,
    total_amount: BigDecimal,
    tax_amount: BigDecimal,
    status: String,
}

fn random_date_in_range(rng: &mut impl Rng, start: NaiveDate, end: NaiveDate) -> NaiveDate {
    let days_in_range = (end - start).num_days();
    start + Duration::days(rng.gen_range(0..=days_in_range))
}

async fn create_invoices_table(pool: &sqlx::Pool<sqlx::Postgres>) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        CREATE TABLE IF NOT EXISTS invoices (
            customer_id INT,
            customer_name TEXT,
            invoice_date TIMESTAMP,
            due_date TIMESTAMP,
            total_amount NUMERIC,
            tax_amount NUMERIC,
            status TEXT
        )
        "#
    )
    .execute(pool)
    .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    create_invoices_table(&pool).await?;

    let mut rng = rand::thread_rng();
    let statuses = vec!["Paid", "Pending", "Overdue"];
    let start_time = Instant::now();

    let batch_size = 10000;
    let mut invoices = Vec::with_capacity(batch_size);

    let start_date = NaiveDate::from_ymd_opt(2021, 1, 1).expect("Invalid start date");
    let end_date = NaiveDate::from_ymd_opt(2024, 6, 16).expect("Invalid end date");

    let args: Vec<String> = env::args().collect();
    let num_invoices: i32 = args.get(1).expect("Please provide the number of invoices to create as a command line argument").parse().expect("The provided argument must be an integer");

    for _ in 0..num_invoices {
        let invoice_date = random_date_in_range(&mut rng, start_date, end_date)
            .and_hms_opt(0, 0, 0)
            .expect("Invalid time");
        let due_date = invoice_date + Duration::days(rng.gen_range(0..=90));

        let total_amount = BigDecimal::from_str(&format!("{:.2}", rng.gen_range(100.0..10000.0)))?;
        let tax_amount = BigDecimal::from_str(&format!("{:.2}", rng.gen_range(10.0..1000.0)))?;

        let invoice = Invoice {
            customer_id: rng.gen_range(1..10000),
            customer_name: Name().fake(),
            invoice_date,
            due_date,
            total_amount,
            tax_amount,
            status: statuses.choose(&mut rng).unwrap().to_string(),
        };

        invoices.push(invoice);

        if invoices.len() == batch_size {
            insert_invoices(&pool, &invoices).await?;
            invoices.clear();
        }
    }

    if !invoices.is_empty() {
        insert_invoices(&pool, &invoices).await?;
    }

    let duration = start_time.elapsed();
    println!("Inserted {} invoices in: {:?}", num_invoices, duration);

    Ok(())
}

async fn insert_invoices(pool: &sqlx::Pool<sqlx::Postgres>, invoices: &[Invoice]) -> Result<(), sqlx::Error> {
    let customer_ids: Vec<i32> = invoices.iter().map(|i| i.customer_id).collect();
    let customer_names: Vec<String> = invoices.iter().map(|i| i.customer_name.clone()).collect();
    let invoice_dates: Vec<NaiveDateTime> = invoices.iter().map(|i| i.invoice_date).collect();
    let due_dates: Vec<NaiveDateTime> = invoices.iter().map(|i| i.due_date).collect();
    let total_amounts: Vec<BigDecimal> = invoices.iter().map(|i| i.total_amount.clone()).collect();
    let tax_amounts: Vec<BigDecimal> = invoices.iter().map(|i| i.tax_amount.clone()).collect();
    let statuses: Vec<String> = invoices.iter().map(|i| i.status.clone()).collect();

    sqlx::query!(
        r#"
        INSERT INTO invoices (customer_id, customer_name, invoice_date, due_date, total_amount, tax_amount, status)
        SELECT * FROM UNNEST(
            $1::int4[],
            $2::text[],
            $3::timestamp[],
            $4::timestamp[],
            $5::numeric[],
            $6::numeric[],
            $7::text[]
        )
        "#,
        &customer_ids,
        &customer_names,
        &invoice_dates,
        &due_dates,
        &total_amounts,
        &tax_amounts,
        &statuses,
    )
    .execute(pool)
    .await?;

    Ok(())
}
