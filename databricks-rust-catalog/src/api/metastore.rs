use super::api_client::APIClient;
use reqwest::{Error, Response};
use serde::Deserialize;

#[derive(Clone)]
pub struct Client {
    api_client: APIClient,
}

impl Client {
    /// Creates metastore client struct
    ///
    /// # Arguments
    ///
    /// * `workspace_name` - The Databricks workspace name containing the metastore access i.e. 'adb-12345678912345.11.azuredatabricks.net'
    /// * `db_token` - The user token to authenticate against Databricks Unity Catalog.
    ///
    /// # Examples
    ///
    /// ```ignore
    ///  let metastore_client: Client = Client::new(workspace_name.clone(), db_token.clone());
    /// ```    
    pub fn new(workspace_name: String, db_token: String) -> Self {
        let api_client: APIClient = APIClient {
            db_token: db_token,
            workspace_name: workspace_name,
        };

        let perms: Client = Client {
            api_client: api_client,
        };

        perms
    }

    /// List all catalogs in a Databricks' Unity Catalog Metastore
    /// - https://docs.databricks.com/api/workspace/catalogs/list
    ///
    /// # Examples
    ///
    /// ```ignore
    ///  client.fetch_catalogs();
    /// ```    
    #[allow(dead_code)]
    async fn fetch_catalogs(&self) -> Result<CatalogResponse, Error> {
        let catalog_url: String = format!(
            "https://{}/api/2.1/unity-catalog/catalogs",
            &self.api_client.workspace_name
        );

        let response: Response = self.api_client.fetch(&catalog_url, None).await?;

        let catalogs: CatalogResponse = match response.json().await {
            Ok(catalogs) => catalogs, // If deserialization succeeds, continue with the deserialized data
            Err(e) => {
                // If deserialization fails, log the error and return an error
                log::error!("Error deserializing JSON response: {}", e);
                return Err(e.into());
            }
        };

        Ok(catalogs)
    }

    /// List schemas fpr a given catalog in a Databricks' Unity Catalog Metastore
    /// - https://docs.databricks.com/api/workspace/schemas/list
    ///
    /// # Examples
    ///
    /// ```ignore
    ///  client.fetch_schemas(catalog_name);
    /// ```  
    #[allow(dead_code)]
    async fn fetch_schemas(
        &self,
        catalog_name: String,
        max_results: Option<usize>,
    ) -> Result<SchemaResponse, Error> {
        let mut schema_url = format!(
            "https://{}/api/2.1/unity-catalog/schemas?catalog_name={}",
            &self.api_client.workspace_name, catalog_name
        );

        if let Some(max) = max_results {
            schema_url.push_str(&format!("&max_results={}", max));
        }

        // Fetch schemas for the current catalog
        let response: Response = self.api_client.fetch(&schema_url, None).await?;
        let schemas: SchemaResponse = match response.json().await {
            Ok(schemas) => schemas, // If deserialization succeeds, continue with the deserialized data
            Err(e) => {
                // If deserialization fails, log the error and return an error
                log::error!("Error deserializing JSON response: {}", e);
                return Err(e.into());
            }
        };

        Ok(schemas)
    }

    /// List all tables for a given schema/catalog in a Databricks' Unity Catalog Metastore
    /// - https://docs.databricks.com/api/workspace/tables/list
    ///
    /// # Examples
    ///
    /// ```ignore
    ///  client.fetch_tables(catalog_name);
    /// ```
    #[allow(dead_code)]
    async fn fetch_tables(
        &self,
        catalog_name: String,
        schema_name: String,
        max_results: Option<usize>,
    ) -> Result<TableResponse, Error> {
        let mut table_url = format!(
            "https://{}/api/2.1/unity-catalog/tables?catalog_name={}&schema_name={}",
            &self.api_client.workspace_name, catalog_name, schema_name
        );

        if let Some(max) = max_results {
            table_url.push_str(&format!("&max_results={}", max));
        }

        // Fetch tables for the current catalog/schema
        let response: Response = self.api_client.fetch(&table_url, None).await?;
        // let tables: TableResponse = response.json().await?;
        let tables: TableResponse = match response.json().await {
            Ok(tables) => tables, // If deserialization succeeds, continue with the deserialized data
            Err(e) => {
                // If deserialization fails, log the error and return an error
                log::error!("Error deserializing JSON response: {}", e);
                return Err(e.into());
            }
        };

        Ok(tables)
    }

    /// Get an individual table object
    /// https://docs.databricks.com/api/workspace/tables/get
    ///
    /// # Examples
    ///
    /// ```ignore
    ///  client.get_table(!format("{}.{}.{}", catalog_name, schema_name, table_name));
    /// ```  
    pub async fn get_table(&self, full_table_name: &str) -> Result<Table, Error> {
        let url: String = format!(
            "https://{}/api/2.1/unity-catalog/tables/{}",
            &self.api_client.workspace_name, full_table_name
        );

        let response: Response = self.api_client.fetch(&url, None).await?;
        let table: Table = response.json().await?;

        Ok(table)
    }

    /// Get an individual schema object
    // https://docs.databricks.com/api/workspace/schemas/get
    ///
    /// # Examples
    ///
    /// ```ignore
    ///  client.get_schema(!format("{}.{}", catalog_name, schema_name));
    /// ```  
    pub async fn get_schema(&self, full_schema_name: String) -> Result<Schema, Error> {
        let url: String = format!(
            "https://{}/api/2.1/unity-catalog/schemas/{}",
            &self.api_client.workspace_name, full_schema_name
        );

        let response: Response = self.api_client.fetch(&url, None).await?;
        let schema: Schema = response.json().await?;

        Ok(schema)
    }

    /// Get an individual catalog object
    /// - https://docs.databricks.com/api/workspace/schemas/get
    ///
    /// # Examples
    ///
    /// ```ignore
    ///  client.get_catalog(catalog_name);
    /// ```  
    pub async fn get_catalog(&self, name: String) -> Result<Catalog, Error> {
        let url: String = format!(
            "https://{}/api/2.1/unity-catalog/catalogs/{}",
            &self.api_client.workspace_name, name
        );

        let response: Response = self.api_client.fetch(&url, None).await?;
        let catalog: Catalog = response.json().await?;

        Ok(catalog)
    }
}

// wrapper struct to contain a vector of catalogs
#[derive(Debug, Deserialize, Clone)]
pub struct CatalogResponse {
    pub catalogs: Vec<Catalog>,
}
impl CatalogResponse {
    pub fn new(catalogs: Vec<Catalog>) -> Self {
        CatalogResponse { catalogs: catalogs }
    }
}

// individual struct for catalogs
#[derive(Debug, Deserialize, Clone)]
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

/// Represents a response containing schemas.
#[derive(Debug, Deserialize, Clone)]
pub struct SchemaResponse {
    /// Optional vector of schemas.
    pub schemas: Option<Vec<Schema>>,
}

impl SchemaResponse {
    /// Constructs a new `SchemaResponse` with the provided vector of schemas.
    /// # Arguments
    /// * `schemas` - A vector of `Schema` instances.
    ///
    /// # Returns
    /// A new `SchemaResponse` instance with the provided schemas.
    pub fn new(schemas: Vec<Schema>) -> Self {
        SchemaResponse {
            schemas: Some(schemas),
        }
    }
}

// represents a schema object in unity catalog
#[derive(Debug, Deserialize, Clone)]
pub struct Schema {
    pub name: String,
    pub catalog_name: String,
    pub owner: String,
    pub comment: Option<String>,
    pub storage_root: Option<String>,
    pub enable_predictive_optimization: Option<String>,
    pub metastore_id: String,
    pub full_name: String,
    pub storage_location: Option<String>,
    pub created_at: i64,
    pub created_by: String,
    pub updated_at: Option<i64>,
    pub updated_by: Option<String>,
    pub catalog_type: Option<String>,
    pub browse_only: Option<bool>,
    pub schema_id: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TableResponse {
    pub tables: Option<Vec<Table>>,
}
impl TableResponse {
    /// Constructs a new `TableResponse` with the provided vector of tables.
    /// # Arguments
    ///
    /// * `tables` - A vector of `Table` instances.
    ///
    /// # Returns
    /// A new `TableResponse` instance with the provided tables.
    pub fn new(tables: Vec<Table>) -> Self {
        TableResponse {
            tables: Some(tables),
        }
    }
}

// represents a table object in unity catalog
#[derive(Debug, Deserialize, Clone)]
pub struct Table {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String,
    pub data_source_format: Option<String>,
    pub storage_location: Option<String>, // full path to table
    pub view_definition: Option<String>,
    pub sql_path: Option<String>,
    pub owner: String,
    pub comment: Option<String>,
    pub storage_credential_name: Option<String>,
    pub enable_predictive_optimization: Option<String>,
    pub metastore_id: Option<String>,
    pub full_name: String,
    pub data_access_configuration_id: Option<String>,
    pub created_at: i64,
    pub created_by: String,
    pub updated_at: Option<i64>,
    pub updated_by: Option<String>,
    pub deleted_at: Option<i64>,
    pub table_id: String,
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
