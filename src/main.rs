use api::permissions::{self};
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
    let metastore_client: api::metastore::MetastoreClient = api::metastore::MetastoreClient { api_client: api_client.clone(), sql_client: sql_client.clone() };
    let permissions_client: api::permissions::Permissions = api::permissions::Permissions { api_client: api_client.clone() };
    // Full Catalog Update - Needs to find better way to just send it to a thread to run in backgorund.
    // let _catalog_update: Result<(), Error> = metastore_client.refresh_catalogs().await;
    // let _schema_update: Result<(), Error> = metastore_client.refresh_all_schemas().await;
    // let _table_update: Result<(), Error> = metastore_client.refresh_all_tables().await;

    // Testing various gets/list/refresh commands
    // let cats = sql_client.list_catalogs(Some("rac")).await;
    // let schs = sql_client.list_schemas(Some("rac_demo_catalog"), Some("product")).await;
    // let ts = sql_client.list_tables(Some("rac_demo_catalog"), Some("productcopy_demo"), Some("clean")).await;

    // Permissions 
    // let active_user: api::permissions::User = permissions_client.authenticate_user("ryan.chynoweth@databricks.com", &api_client.db_token).await?;
    let principal: &str = "ryan.chynoweth@databricks.com";
    let schema_type: permissions::SecurableType = api::permissions::SecurableType::Schema;
    let catalog_type: permissions::SecurableType = api::permissions::SecurableType::Catalog;
    let perms: api::permissions::PrivilegeAssignmentsResponse = permissions_client.fetch_permissions(schema_type.clone(), "rac_demo_catalog.retail_pos", principal).await?;
    // print_permissions(perms).await;

    // let owner = permissions_client.get_object_owner(schema_type.clone(), "rac_demo_catalog.retail_pos").await?;
    // log::info!("{} - {}", owner.full_name, owner.owner);

    let readable: bool = permissions_client.can_read(schema_type.clone(), "rac_demo_catalog.productcopy_demo", principal).await?;
    println!("{}", readable);

    let readable2: bool = permissions_client.can_read(schema_type.clone(), "main.abs_dev", principal).await?;
    println!("{}", readable2);

    let writes: bool = permissions_client.can_write(schema_type.clone(), "main.abs_dev", principal).await?;
    println!("{}", writes);

    
    Ok(())

}

