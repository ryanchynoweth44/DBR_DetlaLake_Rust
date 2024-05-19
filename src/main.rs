use dotenv::dotenv;
use log;
use std::env;
use reqwest::Error;
pub mod sql {
    pub mod sql_client;
}
pub mod api{
    pub mod metastore;
    pub mod permissions;
    pub mod api_client;
}


#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv().ok();
    // env_logger::init();
    env_logger::builder()
    .filter_level(log::LevelFilter::Info)
    .init();

    let db_token: String = env::var("DB_TOKEN").expect("DB_TOKEN not set");
    let workspace_name: String = env::var("WORKSPACE_NAME").expect("WORKSPACE_NAME not set");
    let database_url: String = env::var("DATABASE_URL").expect("DATABASE_URL not set");
    let migrations_path: String = env::var("MIGRATIONS_PATH").expect("MIGRATIONS_PATH not set");


    let api_client: api::api_client::APIClient = api::api_client::APIClient {
        db_token: db_token,
        workspace_name: workspace_name
    };

    // Setup SQL
    let sql_client: sql::sql_client::SqlClient = sql::sql_client::SqlClient::new(&database_url).await.unwrap();
    let _migrate_results = sql_client.run_migrations(&migrations_path).await.unwrap();
    let metastore_sql_client = sql_client.clone();
    let metastore_client: api::metastore::MetastoreClient = api::metastore::MetastoreClient { api_client: api_client, sql_client: metastore_sql_client };
    
    // Full Catalog Update - Needs to find better way to just send it to a thread to run in backgorund.
    // let _catalog_update: Result<(), Error> = metastore_client.refresh_catalogs().await;
    // let _schema_update: Result<(), Error> = metastore_client.refresh_all_schemas().await;
    // let _table_update: Result<(), Error> = metastore_client.refresh_all_tables().await;

    // Testing various gets/list/refresh commands
    // need to get list commands 
    // Then I can start on the reader functions for delta lake. 
    // let cats = sql_client.list_catalogs(Some("rac")).await;
    // let schs = sql_client.list_schemas(Some("rac_demo_catalog"), Some("product")).await;
    let ts = sql_client.list_tables(Some("rac_demo_catalog"), Some("productcopy_demo"), Some("clean")).await;



    Ok(())

}