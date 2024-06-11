use databricks_rust_catalog::data::api_client::APIClient;

use std::env;
use dotenv::dotenv;

#[tokio::test]
async fn test_fetch_method() {
    dotenv().ok();
    let db_token: String = env::var("DB_TOKEN").expect("DB_TOKEN not set");
    let workspace_name: String = env::var("WORKSPACE_NAME").expect("WORKSPACE_NAME not set");

    let client = APIClient{ db_token: db_token.clone(), workspace_name: workspace_name.clone() }; 
    let url: String = format!("https://{}/api/2.0/preview/scim/v2/Me", workspace_name.clone());
    let response = client.fetch(&url).await.unwrap();
    let status: bool = response.status().is_success();
    log::info!("Fetch Status: {}", status);
    assert!(status);
}
