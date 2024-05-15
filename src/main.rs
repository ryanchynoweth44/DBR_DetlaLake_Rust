use dotenv::dotenv;
use std::env;
use reqwest::Error;
pub mod api{
    pub mod metastore;
    pub mod permissions;
    pub mod api_client;
    pub mod sql_client;
}


#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv().ok();
    let db_token: String = env::var("DB_TOKEN").expect("DB_TOKEN not set");
    let workspace_name: String = env::var("WORKSPACE_NAME").expect("WORKSPACE_NAME not set");
    let database_url: String = env::var("DATABASE_URL").expect("DATABASE_URL not set");

    let api_client: api::api_client::APIClient = api::api_client::APIClient {
        db_token: db_token,
        workspace_name: workspace_name
    };

    let metastore_client: api::metastore::MetastoreClient = api::metastore::MetastoreClient{api_client};

    // Testing SQL integration
    let mut sql_client: api::sql_client::SqlClient = api::sql_client::SqlClient::new(&database_url).await.unwrap();
    setup_database(sql_client);
    
    // sql_client.execute_upsert_sql(String::from("catalogs"), String::from("name"));
    

    Ok(())

}


async fn setup_database(mut sql_client: api::sql_client::SqlClient) {

    // sql_client.create_catalogs_table().await.unwrap();
    let catalog_sql: String = sql_client.load_sql_file("C:\\gitmine\\DBR_DetlaLake_Rust\\src\\ddl\\catalogs.sql").await.unwrap();
    let table_sql: String = sql_client.load_sql_file("C:\\gitmine\\DBR_DetlaLake_Rust\\src\\ddl\\tables.sql").await.unwrap();
    let schema_sql: String = sql_client.load_sql_file("C:\\gitmine\\DBR_DetlaLake_Rust\\src\\ddl\\schemas.sql").await.unwrap();

    sql_client.execute_sql(&catalog_sql).await.unwrap();
    sql_client.execute_sql(&schema_sql).await.unwrap();
    sql_client.execute_sql(&table_sql).await.unwrap();
    

}

async fn print_table(metastore_client: api::metastore::MetastoreClient) -> Result<(), Error> {
    let table: api::metastore::Table = metastore_client.get_table(String::from("rac_demo_catalog.productcopy_demo.rac_t5_small_fine_tune_product_copy_payload")).await?;
    println!("Name: {:?}, Owner: {:?}, Comment: {:?}, Location: {:?}, Id: {:?}", 
        table.name,
        table.owner, 
        table.comment, 
        table.storage_location,
        table.table_id,
    );

    Ok(())
}

async fn print_tables(metastore_client: api::metastore::MetastoreClient) -> Result<(), Error> {
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

async fn print_schemas(metastore_client: api::metastore::MetastoreClient) -> Result<(), Error> {
    let schemas: api::metastore::SchemaResponse = metastore_client.fetch_schemas(String::from("rac_demo_catalog"), None).await?;
    if let Some(schemas) = schemas.schemas { // checks to see if something is not none
        for obj in schemas {
            println!("Name: {:?}, Owner: {:?}, Comment: {:?}, Schema ID: {:?}", 
                obj.name,
                obj.owner, 
                obj.comment, 
                obj.schema_id
            );
        }
    }
    Ok(())
}

async fn print_catalogs(metastore_client: api::metastore::MetastoreClient) -> Result<(), Error> {
    let catalogs: api::metastore::CatalogResponse = metastore_client.fetch_catalogs().await?;
    for obj in catalogs.catalogs {
        println!("Name: {:?}, Owner: {:?}, Comment: {:?}", 
            obj.name,
            obj.owner, 
            obj.comment, 
        );
    }
    Ok(())
}