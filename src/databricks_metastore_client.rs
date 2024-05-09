use reqwest::{header::HeaderMap, Response, Error};
use serde::Deserialize;

pub struct DatabricksMetastoreClient {
    pub db_token: String,
    pub workspace_name: String,
}

impl DatabricksMetastoreClient {

    pub async fn fetch_catalogs(&self) -> Result<CatalogResponse, Error>  {
        let url: String = format!("https://{}/api/2.1/unity-catalog/catalogs", &self.workspace_name);
        
        let client: reqwest::Client = reqwest::Client::new();
        let mut headers: HeaderMap = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("Authorization", format!("Bearer {}", &self.db_token).parse().unwrap());
        

        let response: Response = client.get(&url)
        .headers(headers.clone())
        .send()
        .await?;

        let catalogs: CatalogResponse = response.json().await?;
        
        Ok(catalogs)
    }

}

#[derive(Debug, Deserialize)]
pub struct CatalogResponse {
    pub catalogs: Vec<Catalog>,
  }

#[derive(Debug, Deserialize)]
pub struct Catalog {
    pub name: Option<String>,
    pub owner: Option<String>,
    pub comment: Option<String>,
    pub storage_root: Option<String>,
    pub provider_name: Option<String>,
    pub share_name: Option<String>,
    pub enable_predictive_optimization: Option<String>,
    pub metastore_id: Option<String>,
    pub created_at: Option<u64>, // Assuming "timestamp" is a string representation of timestamp
    pub created_by: Option<String>,
    pub updated_at: Option<u64>, // Assuming "timestamp" is a string representation of timestamp
    pub updated_by: Option<String>,
    pub catalog_type: Option<String>,
    pub storage_location: Option<String>,
    pub isolation_mode: Option<String>,
    pub connection_name: Option<String>,
    pub full_name: Option<String>,
    pub securable_kind: Option<String>,
    pub securable_type: Option<String>,
    pub browse_only: Option<bool>,
    // excluded fields due to nesting
    // properties
    // effective_predictive_optimization_flag
    // options
    // provisioning_info
}