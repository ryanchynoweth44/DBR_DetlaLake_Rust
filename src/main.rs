use dotenv::dotenv;
use std::env;
use reqwest::Error;
// use serde::Deserialize;
mod databricks_metastore_client;
use databricks_metastore_client::DatabricksMetastoreClient;



#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv().ok();
    let db_token: String = env::var("DB_TOKEN").expect("DB_TOKEN not set");
    let workspace_name: String = env::var("WORKSPACE_NAME").expect("WORKSPACE_NAME not set");

    let db_metastore_client: DatabricksMetastoreClient = DatabricksMetastoreClient{db_token:db_token, workspace_name:workspace_name};
    let catalogs: databricks_metastore_client::CatalogResponse = db_metastore_client.fetch_catalogs().await?;

    for catalog in catalogs.catalogs {
        println!("Owner: {:?}, Comment: {:?}, Storage Root: {:?}, Provider Name: {:?}, Share Name: {:?}, Enable Predictive Optimization: {:?}, Metastore ID: {:?}, Created At: {:?}, Created By: {:?}, Updated At: {:?}, Updated By: {:?}, Catalog Type: {:?}, Storage Location: {:?}, Isolation Mode: {:?}, Connection Name: {:?}, Full Name: {:?}, Securable Kind: {:?}, Securable Type: {:?}, Browse Only: {:?}", 
            catalog.owner, 
            catalog.comment, 
            catalog.storage_root, 
            catalog.provider_name, 
            catalog.share_name, 
            catalog.enable_predictive_optimization, 
            catalog.metastore_id, 
            catalog.created_at, 
            catalog.created_by, 
            catalog.updated_at, 
            catalog.updated_by, 
            catalog.catalog_type, 
            catalog.storage_location, 
            catalog.isolation_mode, 
            catalog.connection_name, 
            catalog.full_name, 
            catalog.securable_kind, 
            catalog.securable_type, 
            catalog.browse_only
        );
    }
    

    Ok(())

}
