//https://github.com/delta-io/delta-rs
use deltalake::{open_table_with_storage_options, DeltaTableError, datafusion::prelude::SessionContext, Path, ObjectStore};
use deltalake::azure::register_handlers;
use std::sync::Arc;
use std::collections::HashMap;
use polars::prelude::*;
use std::io::Cursor;
use std::convert::TryFrom;


pub struct DeltaLakeReader {
    pub storage_credentials: AzureDataLakeGen2Options,
}
impl DeltaLakeReader {
    pub fn new(storage_credentials: AzureDataLakeGen2Options) -> Self {
        let mut reader: DeltaLakeReader = DeltaLakeReader {
            storage_credentials,
        };

        // Call the register_handlers function
        register_handlers(None);

        reader
    }

    pub async fn read_delta_table_as_datafusion(&self, table_path: &str) -> Result<deltalake::datafusion::prelude::DataFrame, DeltaTableError>{
        log::info!("Reading Table: {}", table_path);
        let table: deltalake::DeltaTable = open_table_with_storage_options(table_path, self.storage_credentials.to_hash_map()).await?;

        let ctx: SessionContext = SessionContext::new();

        ctx.register_table("loadtable", Arc::new(table)).unwrap();

        let df: deltalake::datafusion::prelude::DataFrame = ctx.sql("SELECT * FROM loadtable").await.unwrap();

        Ok(df)
    }


    pub async fn read_delta_table_as_polars(&self, table_path: &str) -> Result<polars::prelude::DataFrame, DeltaTableError> { //Result<polars::prelude::DataFrame, DeltaTableError> {
        log::info!("Reading Table: {}", table_path);
        let table: deltalake::DeltaTable = open_table_with_storage_options(table_path, self.storage_credentials.to_hash_map()).await?;

        let files: Vec<String> = table.get_file_uris().unwrap().collect();
        let files_clone = files.clone();
        let object_store: Arc<dyn ObjectStore> = table.object_store();

        let parts: Vec<&str> = files_clone[0].split('/').collect();
        let file_name: &str = parts[parts.len()-1];
        let file_path: Path = Path::try_from(format!("{}", file_name)).unwrap();
        let data: deltalake::storage::GetResult = object_store.get(&file_path).await.unwrap();
        let bytes = data.bytes().await.unwrap();
        // Load the bytes into an in-memory buffer
        let cursor = Cursor::new(bytes);
        // Read the Parquet data into a Polars DataFrame
        let mut df: DataFrame = ParquetReader::new(cursor).finish().unwrap();

        for file in files.iter().skip(1) {
            log::info!("Loading file: {}", file);
            let parts: Vec<&str> = file.split('/').collect();
            let file_name: &str = parts[parts.len()-1];
            let file_path: Path = Path::try_from(format!("{}", file_name)).unwrap();
            let data: deltalake::storage::GetResult = object_store.get(&file_path).await.unwrap();
            let bytes = data.bytes().await.unwrap();
            // Load the bytes into an in-memory buffer
            let cursor = Cursor::new(bytes);
            let new_df: DataFrame = ParquetReader::new(cursor).finish().unwrap();

            df = df.vstack(&new_df).unwrap();
        }
    
        // Print the DataFrame
        println!("{:?}", df);
        


        Ok(df)
    }


    pub async fn print_datafusion_dataframe(&self, df: deltalake::datafusion::prelude::DataFrame) {
        let data = df.collect().await.unwrap();

        for d in data {
            log::info!("{:?}", d);

        }
    }


}

// pub struct Writer {
//     placeholder: String,
// }


// https://delta-io.github.io/delta-rs/usage/loading-table/
// https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants
#[derive(Debug, Clone)]
pub struct AzureDataLakeGen2Options {
    pub azure_storage_account_name: String, 
    // pub azure_container_name: String, // make this optional??
    pub azure_client_id: String,
    pub azure_client_secret: String,
    pub azure_tenant_id: String 
}
impl AzureDataLakeGen2Options {
    pub fn to_hash_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("azure_storage_account_name".to_string(), self.azure_storage_account_name.clone());
        map.insert("azure_client_id".to_string(), self.azure_client_id.clone());
        map.insert("azure_client_secret".to_string(), self.azure_client_secret.clone());
        map.insert("azure_tenant_id".to_string(), self.azure_tenant_id.clone());
        map
    }
}