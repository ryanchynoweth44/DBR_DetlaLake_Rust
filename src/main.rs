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

    let metastore_client: api::metastore::MetastoreClient = api::metastore::MetastoreClient{api_client};

    // Testing SQL integration
    let sql_client: sql::sql_client::SqlClient = sql::sql_client::SqlClient::new(&database_url, migrations_path).await.unwrap();
    let _db_setup = setup_database(&sql_client).await;
    let _catalog_update = update_catalogs(&sql_client, &metastore_client).await;
    let _schema_update = update_schemas(&sql_client, &metastore_client).await;
    

    Ok(())

}


async fn setup_database(sql_client: &sql::sql_client::SqlClient) -> () {

    let migrate_results = sql_client.run_migrations().await.unwrap();    
    migrate_results

}

async fn update_catalogs(sql_client: &sql::sql_client::SqlClient, metastore_client: &api::metastore::MetastoreClient) -> Result<(), Error> {
    log::info!("Getting Catalogs. ");
    let catalogs: api::metastore::CatalogResponse = metastore_client.fetch_catalogs().await?;
    sql_client.write_catalogs(catalogs).await.unwrap();

    Ok(())
}


// there appears to be an error when saving the data to the database. 
async fn update_schemas(sql_client: &sql::sql_client::SqlClient, metastore_client: &api::metastore::MetastoreClient) -> Result<(), Error> {
    let catalogs: api::metastore::CatalogResponse = metastore_client.fetch_catalogs().await?;
    log::info!("Getting Schemas. Length: {}", catalogs.catalogs.len());
    for catalog in catalogs.catalogs {
        log::info!("{} | {}", catalog.name, catalog.catalog_type);
        if catalog.catalog_type != "DELTASHARING_CATALOG" {
            let schemas: api::metastore::SchemaResponse = metastore_client.fetch_schemas(catalog.name, None).await?;
            sql_client.write_schemas(schemas).await.unwrap();
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
    Ok(())
}

// async fn print_schemas(metastore_client: api::metastore::MetastoreClient) -> Result<(), Error> {
//     let schemas: api::metastore::SchemaResponse = metastore_client.fetch_schemas(String::from("rac_demo_catalog"), None).await?;
//     if let Some(schemas) = schemas.schemas { // checks to see if something is not none
//         for obj in schemas {
//             println!("Name: {:?}, Owner: {:?}, Comment: {:?}, Schema ID: {:?}", 
//                 obj.name,
//                 obj.owner, 
//                 obj.comment, 
//                 obj.schema_id
//             );
//         }
//     }
//     Ok(())
// }