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
    let schemas: databricks_metastore_client::SchemaResponse = db_metastore_client.fetch_schemas(String::from("main")).await?;
    let all_schemas: databricks_metastore_client::SchemaResponse = db_metastore_client.fetch_all_schemas().await?;
    
    if let Some(schemas) = all_schemas.schemas { // checks to see if something is not none
        for obj in schemas {
            println!("Name: {:?}, Owner: {:?}, Comment: {:?}", 
                obj.name,
                obj.owner, 
                obj.comment, 
            );
        }
    }
    

    Ok(())

}
