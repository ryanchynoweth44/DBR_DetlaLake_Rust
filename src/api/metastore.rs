use reqwest::{Response, Error};
use serde::Deserialize;
use super::api_client::APIClient;

// MetastoreClient responsible for data collection 
pub struct MetastoreClient {
    pub api_client: APIClient,

}

impl MetastoreClient {

    // List all catalogs in a Databricks' Unity Catalog Metastore
    // https://docs.databricks.com/api/workspace/catalogs/list
    pub async fn fetch_catalogs(&self) -> Result<CatalogResponse, Error>  {
        let catalog_url: String = format!("https://{}/api/2.1/unity-catalog/catalogs", &self.api_client.workspace_name);

        let response: Response = self.api_client.fetch(catalog_url).await?;

        let catalogs: CatalogResponse = response.json().await?;
        
        Ok(catalogs)
    }


    // List schemas fpr a given catalog in a Databricks' Unity Catalog Metastore
    // https://docs.databricks.com/api/workspace/schemas/list
    pub async fn fetch_schemas(&self, catalog_name: String, max_results: Option<usize>) -> Result<SchemaResponse, Error>  {
        let mut schema_url = format!("https://{}/api/2.1/unity-catalog/schemas?catalog_name={}", &self.api_client.workspace_name, catalog_name);
        
        if let Some(max) = max_results {
            schema_url.push_str(&format!("&max_results={}", max));
        }
        
        // Fetch schemas for the current catalog
        let response: Response = self.api_client.fetch(schema_url).await?;
        let schemas: SchemaResponse = response.json().await?;
        
        Ok(schemas)
    }

    // List all tables for a given schema/catalog in a Databricks' Unity Catalog Metastore
    // https://docs.databricks.com/api/workspace/tables/list
    pub async fn fetch_tables(&self, catalog_name: String, schema_name: String, max_results: Option<usize>) -> Result<TableResponse, Error>  {
        let mut table_url = format!("https://{}/api/2.1/unity-catalog/tables?catalog_name={}&schema_name={}", &self.api_client.workspace_name, catalog_name, schema_name);

        if let Some(max) = max_results {
            table_url.push_str(&format!("&max_results={}", max));
        }

        // Fetch tables for the current catalog/schema
        let response: Response = self.api_client.fetch(table_url.clone()).await?;
        let tables: TableResponse = response.json().await?;
        
        Ok(tables)
    }

    // Get an individual table object
    // https://docs.databricks.com/api/workspace/tables/get
    pub async fn get_table(&self, full_table_name: String) -> Result<Table, Error>  {
        let table_url = format!("https://{}/api/2.1/unity-catalog/tables/{}", &self.api_client.workspace_name, full_table_name);

        let response: Response = self.api_client.fetch(table_url.clone()).await?;
        let table: Table = response.json().await?;
        
        Ok(table)
    }

}

// wrapper struct to contain a vector of catalogs
#[derive(Debug, Deserialize)]
pub struct CatalogResponse {
    pub catalogs: Vec<Catalog>,
  }


// individual struct for catalogs
#[derive(Debug, Deserialize)]
pub struct Catalog {
    pub name: String,
    pub owner: String,
    pub comment: Option<String>,
    pub storage_root: Option<String>,
    pub provider_name: Option<String>,
    pub share_name: Option<String>,
    pub enable_predictive_optimization: Option<String>,
    pub metastore_id: String,
    pub created_at: i64, 
    pub created_by: String,
    pub updated_at: Option<i64>, 
    pub updated_by: Option<String>,
    pub catalog_type: String,
    pub storage_location: Option<String>,
    pub isolation_mode: Option<String>,
    pub connection_name: Option<String>,
    pub full_name: String,
    pub securable_kind: Option<String>,
    pub securable_type: Option<String>,
    pub browse_only: Option<bool>,
    // excluded fields due to nesting
    // properties
    // effective_predictive_optimization_flag
    // options
    // provisioning_info
}



#[derive(Debug, Deserialize)]
pub struct SchemaResponse {
    pub schemas: Vec<Schema>,
  }


#[derive(Debug, Deserialize)]
pub struct Schema {
    pub name: String,
    pub catalog_name: String,
    pub owner: String,
    pub comment: Option<String>,
    pub storage_root: Option<String>,
    pub enable_predictive_optimization: Option<String>, 
    pub metastore_id:String,
    pub full_name:String,
    pub storage_location:Option<String>,
    pub created_at: i64,
    pub created_by:String,
    pub updated_at: Option<i64>,
    pub updated_by:Option<String>,
    pub catalog_type:Option<String>,
    pub browse_only:Option<bool>,
    pub schema_id:String,

}


#[derive(Debug, Deserialize)]
pub struct TableResponse {
    pub tables: Vec<Table>,
  }


#[derive(Debug, Deserialize)]
pub struct Table {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String,
    pub data_source_format: String,
    pub storage_location: String, // full path to table
    pub view_definition: Option<String>,
    pub sql_path: Option<String>,
    pub owner: String,
    pub comment: Option<String>,
    pub storage_credential_name: Option<String>,
    pub enable_predictive_optimization: Option<String>,
    pub metastore_id: Option<String>,
    pub full_name: String,
    pub data_access_configuration_id: Option<String>,
    pub created_at: u64,
    pub created_by: String,
    pub updated_at: Option<u64>,
    pub updated_by: Option<String>,
    pub deleted_at: Option<u64>,
    pub table_id: Option<String>,
    pub access_point: Option<String>,
    pub pipeline_id: Option<String>,
    pub browse_only: Option<bool>,
    // excluded fields due to nesting
    // columns
    // dependencies 
    // properties
    // table_constraints
    // row_filter
    // delta_runtime_properties_kvpairs
    // effective_predictive_optimization_flag
}
    