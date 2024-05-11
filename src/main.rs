use dotenv::dotenv;
use std::env;
use reqwest::Error;
pub mod api{
    pub mod metastore;
    pub mod permissions;
    pub mod api_client;
}
// use api::metastore::MetastoreClient;



#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv().ok();
    let db_token: String = env::var("DB_TOKEN").expect("DB_TOKEN not set");
    let workspace_name: String = env::var("WORKSPACE_NAME").expect("WORKSPACE_NAME not set");

    let api_client: api::api_client::APIClient = api::api_client::APIClient {
        db_token: db_token,
        workspace_name: workspace_name
    };

    let metastore_client: api::metastore::MetastoreClient = api::metastore::MetastoreClient{api_client};
    let catalogs: api::metastore::CatalogResponse = metastore_client.fetch_catalogs().await?;
    let schemas: api::metastore::SchemaResponse = metastore_client.fetch_schemas(String::from("rac_demo_catalog"), None).await?;
    // let all_schemas: databricks_metastore_client::SchemaResponse = db_metastore_client.fetch_all_schemas().await?;

    if let Some(schemas) = schemas.schemas { // checks to see if something is not none
        for obj in schemas {
            println!("Name: {:?}, Owner: {:?}, Comment: {:?}", 
                obj.name,
                obj.owner, 
                obj.comment, 
            );
        }
    }

    let tables: api::metastore::TableResponse = metastore_client.fetch_tables(String::from("rac_demo_catalog"), String::from("productcopy_demo"), None).await?;
    for obj in tables.tables {
        println!("Name: {:?}, Owner: {:?}, Comment: {:?}, Location: {:?}, Id: {:?}", 
            obj.name,
            obj.owner, 
            obj.comment, 
            obj.storage_location,
            obj.table_id,
        );
    }
    

    Ok(())

}
