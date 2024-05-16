use dotenv::dotenv;
// use log; //https://github.com/rust-lang/log/issues/376
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
    let db_setup = setup_database(&sql_client).await;
    write_catalogs(&sql_client, metastore_client).await;
    

    Ok(())

}


async fn setup_database(sql_client: &sql::sql_client::SqlClient) -> () {

    let migrate_results = sql_client.run_migrations().await.unwrap();    
    migrate_results

}

async fn write_catalogs(sql_client: &sql::sql_client::SqlClient, metastore_client: api::metastore::MetastoreClient) -> Result<(), sqlx::Error> {

    let catalogs: api::metastore::CatalogResponse = metastore_client.fetch_catalogs().await.unwrap();
    sql_client.write_catalog(catalogs).await.unwrap();

    Ok(())
}
