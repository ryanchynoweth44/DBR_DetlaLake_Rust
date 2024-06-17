#![warn(unused_extern_crates)]
use dotenv::dotenv;
use log;
use std::env;
use std::error::Error;
pub mod api {
    pub mod api_client;
    pub mod delta;
    pub mod metastore;
    pub mod permissions;
}

use api::delta::DeltaLakeManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let db_token: String = env::var("DB_TOKEN").expect("DB_TOKEN not set");
    let workspace_name: String = env::var("WORKSPACE_NAME").expect("WORKSPACE_NAME not set");
    let principal: String = env::var("USER_NAME").expect("USER_NAME not set");

    // Data Reading
    let reader: DeltaLakeManager =
        DeltaLakeManager::new(principal, db_token, workspace_name).await?;

    let table_name: &str = "rac_demo_catalog.rust_schema.dbu_forecasts";

    // let df = reader.read_delta_table_as_datafusion(table_path).await.unwrap();

    let pdf: polars::prelude::DataFrame = reader
        .read_delta_table_as_polars(table_name, true)
        .await
        .unwrap();
    println!("{}", pdf);

    Ok(())
}
