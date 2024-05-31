use api::permissions::{self};
use dotenv::dotenv;
use log;
use std::{env, ptr::read};
use reqwest::Error;
pub mod sql {
    pub mod sql_client;
}
pub mod api{
    pub mod metastore;
    pub mod permissions;
    pub mod api_client;
}
pub mod data { 
    pub mod data;
}
use data::data::{AzureDataLakeGen2Options, DeltaLakeReader};


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
    let azure_storage_account_name: String = env::var("AZURE_STORAGE_ACCOUNT_NAME").expect("AZURE_STORAGE_ACCOUNT_NAME not set");
    let azure_storage_container: String = env::var("AZURE_STORAGE_CONTAINER").expect("AZURE_STORAGE_CONTAINER not set");
    let azure_client_id: String = env::var("AZURE_CLIENT_ID").expect("AZURE_CLIENT_ID not set");
    let azure_client_secret: String = env::var("AZURE_CLIENT_SECRET").expect("AZURE_CLIENT_SECRET not set");
    let azure_tenant_id: String = env::var("AZURE_TENANT_ID").expect("AZURE_TENANT_ID not set");


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

    ////////// Permissions 
    // let active_user: api::permissions::User = permissions_client.authenticate_user("ryan.chynoweth@databricks.com", &api_client.db_token).await?;
    let principal: &str = "ryan.chynoweth@databricks.com";
    let schema_type: permissions::SecurableType = api::permissions::SecurableType::Schema;
    let catalog_type: permissions::SecurableType = api::permissions::SecurableType::Catalog;
    // let perms: api::permissions::PrivilegeAssignmentsResponse = permissions_client.fetch_permissions(schema_type.clone(), "rac_demo_catalog.retail_pos", principal).await?;
    // let owner = permissions_client.get_object_owner(schema_type.clone(), "rac_demo_catalog.retail_pos").await?;
    // log::info!("{} - {}", owner.full_name, owner.owner);

    // let readable: bool = permissions_client.can_read(schema_type.clone(), "rac_demo_catalog.productcopy_demo", principal).await?;
    // println!("{}", readable);

    // let readable2: bool = permissions_client.can_read(schema_type.clone(), "main.abs_dev", principal).await?;
    // println!("{}", readable2);

    // let writes: bool = permissions_client.can_write(schema_type.clone(), "main.abs_dev", principal).await?;
    // println!("{}", writes);

    /////////// Data Reading
    let table_path: &str = &format!("abfs://{}/__unitystorage/schemas/326fc918-bcd6-43e0-ab85-d7afd4e0cc9b/tables/07310682-a42f-4896-b272-935a515abb0e", azure_storage_container.clone());
    let storage_options: AzureDataLakeGen2Options = AzureDataLakeGen2Options { azure_storage_account_name: azure_storage_account_name.clone(), 
        azure_client_id: azure_client_id, 
        azure_client_secret : azure_client_secret, 
        azure_tenant_id : azure_tenant_id
    };
    let reader = DeltaLakeReader::new(storage_options);

    let df = reader.read_delta_table_as_datafusion(table_path).await.unwrap();

    let pdf = reader.read_delta_table_as_polars(table_path).await.unwrap();

    Ok(())

}

