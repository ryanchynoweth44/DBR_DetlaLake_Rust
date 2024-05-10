use reqwest::{header::HeaderMap, Response, Error};
use serde::Deserialize;

pub struct DatabricksMetastoreClient {
    pub db_token: String,
    pub workspace_name: String,
}

impl DatabricksMetastoreClient {

    async fn fetch(&self, url: String) -> Result<Response, Error> {
        let client: reqwest::Client = reqwest::Client::new();
        let mut headers: HeaderMap = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("Authorization", format!("Bearer {}", &self.db_token).parse().unwrap());
        

        let response: Response = client.get(&url)
        .headers(headers.clone())
        .send()
        .await?;

        Ok(response)
    }

    // https://docs.databricks.com/api/workspace/catalogs/list
    pub async fn fetch_catalogs(&self) -> Result<CatalogResponse, Error>  {
        let catalog_url: String = format!("https://{}/api/2.1/unity-catalog/catalogs", &self.workspace_name);

        let response: Response = self.fetch(catalog_url).await?;

        let catalogs: CatalogResponse = response.json().await?;
        
        Ok(catalogs)
    }

    pub async fn fetch_all_schemas(&self) -> Result<SchemaResponse, Error> {
        // this needs to change so that each batch saves to DB instead of returning a massive object? 
        let catalogs: Vec<Catalog> = self.fetch_catalogs().await?.catalogs;
        let mut schema_response: SchemaResponse = SchemaResponse::new(); // Create an empty SchemaResponse object


        for catalog in catalogs {
            let schema_url: String = format!("https://{}/api/2.1/unity-catalog/schemas?catalog_name={:?}", &self.workspace_name, catalog.name);

            // Fetch schemas for the current catalog
            let response: Response = self.fetch(schema_url).await?;
            let schemas: SchemaResponse = response.json().await?;

            // Add schemas to the vector
            // schema_response.extend_schemas(schemas.schemas);           
            if let Some(schemas) = schemas.schemas {
                schema_response.extend_schemas(schemas);
            }
        }

        Ok(schema_response)
    }

    // https://docs.databricks.com/api/workspace/schemas/list
    pub async fn fetch_schemas(&self, catalog_name: String, max_results: Option<usize>) -> Result<SchemaResponse, Error>  {
        let mut schema_url = format!("https://{}/api/2.1/unity-catalog/schemas?catalog_name={}", &self.workspace_name, catalog_name);
        
        if let Some(max) = max_results {
            schema_url.push_str(&format!("&max_results={}", max));
        }
        
        // Fetch schemas for the current catalog
        let response: Response = self.fetch(schema_url).await?;
        let schemas: SchemaResponse = response.json().await?;
        
        Ok(schemas)
    }

    // https://docs.databricks.com/api/workspace/tables/list
    pub async fn fetch_tables(&self, catalog_name: String, schema_name: String, max_results: Option<usize>) -> Result<TableResponse, Error>  {
        let mut table_url = format!("https://{}/api/2.1/unity-catalog/tables?catalog_name={}&schema_name={}", &self.workspace_name, catalog_name, schema_name);

        if let Some(max) = max_results {
            table_url.push_str(&format!("&max_results={}", max));
        }

        // Fetch schemas for the current catalog
        let response: Response = self.fetch(table_url.clone()).await?;
        let tables: TableResponse = response.json().await?;
        
        Ok(tables)
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



#[derive(Debug, Deserialize)]
pub struct SchemaResponse {
    pub schemas: Option<Vec<Schema>>,
  }
impl SchemaResponse {
// Default constructor method
    fn new() -> Self {
        Self {
            schemas: None,
        }
    }

    // Method to extend the 'schemas' field
    fn extend_schemas(&mut self, new_schemas: Vec<Schema>) {
        // Check if 'schemas' is Some
        if let Some(existing_schemas) = &mut self.schemas {
            // Extend the existing schemas with new schemas
            existing_schemas.extend(new_schemas);
        } else {
            // If 'schemas' is None, set it to Some containing the new schemas
            self.schemas = Some(new_schemas);
        }
    }
}


#[derive(Debug, Deserialize)]
pub struct Schema {
    pub name: Option<String>,
    pub catalog_name: Option<String>,
    pub owner: Option<String>,
    pub comment: Option<String>,
    pub storage_root: Option<String>,
    pub enable_predictive_optimization: Option<String>, 
    pub metastore_id:Option<String>,
    pub full_name:Option<String>,
    pub storage_location:Option<String>,
    pub created_at: Option<u64>,
    pub created_by:Option<String>,
    pub updated_at: Option<u64>,
    pub updated_by:Option<String>,
    pub catalog_type:Option<String>,
    pub browse_only:Option<bool>,
    pub schema_id:Option<String>,

}


#[derive(Debug, Deserialize)]
pub struct TableResponse {
    pub tables: Vec<Table>,
  }


#[derive(Debug, Deserialize)]
pub struct Table {
    pub name: Option<String>,
    pub catalog_name: Option<String>,
    pub schema_name: Option<String>,
    pub table_type: Option<String>,
    pub data_source_format: Option<String>,
    pub storage_location: Option<String>, // full path to table
    pub view_definition: Option<String>,
    pub sql_path: Option<String>,
    pub owner: Option<String>,
    pub comment: Option<String>,
    pub storage_credential_name: Option<String>,
    pub enable_predictive_optimization: Option<String>,
    pub metastore_id: Option<String>,
    pub full_name: Option<String>,
    pub data_access_configuration_id: Option<String>,
    pub created_at: Option<u64>,
    pub created_by: Option<String>,
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
    