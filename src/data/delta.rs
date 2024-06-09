//https://github.com/delta-io/delta-rs
use deltalake::{
    open_table_with_storage_options, 
    DeltaTable,
    datafusion::prelude::DataFrame as DatafusionDataFrame, 
    datafusion::prelude::*,
    Path, 
    ObjectStore,
    azure::register_handlers
};
use polars::prelude::{
    DataFrame as PolarsDataFrame,
    *,
};


use std::path::PathBuf;
use std::error::Error;
use std::sync::Arc;
use std::io::Cursor;
use std::convert::TryFrom;
use bytes::Bytes; 
use futures;



use super::api_client::APIClient;
use super::permissions;
use super::permissions::AzureDataLakeGen2Options; 
use super::metastore::*;


pub struct DeltaLakeManager {
    storage_credentials: AzureDataLakeGen2Options,
    api_client: APIClient,
    metastore_client: Client,
    principal: String,

}
impl DeltaLakeManager {
    /// Creates the delta lake reader struct
    ///
    /// # Arguments
    ///
    /// * `storage_credentials` - The credentials used to authenticate against azure storage
    /// * `api_client` - API Client to validate user permissions against unity catalog 
    /// * `metastore_client` - Metastore Client object to interact with Unity Catalog APIs for data objects. 
    /// * `principal` - The active user's username. 
    ///
    /// # Examples
    ///
    /// ```
    ///  let reader: DeltaLakeManager = DeltaLakeManager::new(principal, db_token, workspace_name).await;
    /// ```
    pub async fn new(principal: String, db_token: String, workspace_name: String) -> Result<Self, Box<dyn Error>> {
        let api_client: APIClient = APIClient{
            db_token: db_token.clone(),
            workspace_name: workspace_name.clone()
        };


        let storage_credentials: AzureDataLakeGen2Options = permissions::authenticate_user(api_client.clone(), &principal, &db_token, &workspace_name).await?;

        let metastore_client: Client = Client::new(workspace_name.clone(), db_token.clone());


        let reader: DeltaLakeManager = DeltaLakeManager {
            storage_credentials,
            // permissions_client,
            api_client,
            metastore_client,
            principal,
        };

        // Call the register_handlers function
        register_handlers(None);

        Ok(reader)
    }

    /// If the user has permission to read the table, then this function returns a datafusion dataframe. 
    ///
    /// # Arguments
    ///
    /// * `table_name` - The fully qualified table name
    ///
    /// # Examples
    ///
    /// ```
    /// let table_name: &str = "my_catalog.my_schema.my_table";
    /// let df = reader.read_delta_table_as_datafusion(table_path).await?;
    /// ```
    pub async fn read_delta_table_as_datafusion(&self, table_name: &str) -> Result<DatafusionDataFrame, Box<dyn Error>> {
        let table_path: String = self.metastore_client.get_table(table_name).await?.storage_location.ok_or("Table Location Not Found.")?;
        if !permissions::can_read(self.api_client.clone(), &table_name, &self.principal).await? {
            log::error!("Permissions on Object {} Denied.", table_name);
            return Err(Box::<dyn Error>::from("Permission Denied."));
        } else {
            log::info!("Validated Permissions on Object: {}", table_name);

            log::info!("Reading Table: {}", table_path);
            let table: DeltaTable = open_table_with_storage_options(table_path, self.storage_credentials.to_hash_map()).await?;

            let ctx: SessionContext = SessionContext::new();

            ctx.register_table("loadtable", Arc::new(table))?;

            let df: DatafusionDataFrame = ctx.sql("SELECT * FROM loadtable").await?;
            return Ok(df);
        }

        
    }

    /// Reads a delta table in a parallel fashion
    ///
    /// # Arguments
    ///
    /// * `table_path` - The path to the table in cloud storage
    ///
    /// # Examples
    ///
    /// ```
    /// let table_path: String = self.metastore_client.get_table(table_name).await?.storage_location.ok_or("Table Location Not Found.")?;
    /// let table_bytes = self.parallel_read_table_as_bytes(&table_path).await?;
    /// ```
    async fn parallel_read_table_as_bytes(&self, table_path: &str) -> Result<Vec<Bytes>, Box<dyn Error>> {
        log::info!("Reading Table: {}", table_path);
        let table: DeltaTable = open_table_with_storage_options(table_path, self.storage_credentials.to_hash_map()).await?;
    
        let files: Vec<String> = table.get_file_uris()?.collect();
        let object_store: Arc<dyn ObjectStore> = table.object_store();
    
        let futures: Vec<_> = files.into_iter().map(|file| {
            let object_store = Arc::clone(&object_store);
            async move {
                log::info!("Loading file: {}", file);
                let parts: Vec<&str> = file.split('/').collect();
                let file_name: &str = parts.last().ok_or("Error Parsing File URIs")?;
                let file_path: Path = Path::try_from(file_name.to_string())?;
                let result = object_store.get(&file_path).await?;
                let bytes = result.bytes().await?;
                Ok::<Bytes, Box<dyn Error>>(bytes)
            }
        }).collect();
    
        let table_bytes: Vec<Bytes> = futures::future::join_all(futures).await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
    
        Ok(table_bytes)
    }

    /// Reads a delta table in a serial fashion
    ///
    /// # Arguments
    ///
    /// * `table_path` - The path to the table in cloud storage
    ///
    /// # Examples
    ///
    /// ```
    /// let table_path: String = self.metastore_client.get_table(table_name).await?.storage_location.ok_or("Table Location Not Found.")?;
    /// let table_bytes = self.read_table_as_bytes(&table_path).await?;
    /// ```
    async fn read_table_as_bytes(&self, table_path: &str) -> Result<Vec<Bytes>, Box<dyn Error>> { // return bytes
        log::info!("Reading Table: {}", table_path);
        let table: DeltaTable = open_table_with_storage_options(table_path, self.storage_credentials.to_hash_map()).await?;

        let mut table_bytes: Vec<Bytes> = Vec::default();

        // get the files and storage object
        let files: Vec<String> = table.get_file_uris()?.collect();
        let object_store: Arc<dyn ObjectStore> = table.object_store();

        // foreach file we need only the file name 
        // provide it to the storage objet to download into bytes 
        // load the bytes into a Vec<Bytes>
        for file in files.iter() {
            log::info!("Loading file: {}", file);
            let parts: Vec<&str> = file.split('/').collect();
            let file_name: &str = parts[parts.len()-1];
            let file_path: Path = Path::try_from(format!("{}", file_name))?;
            let result: deltalake::storage::GetResult = object_store.get(&file_path).await?;
            let bytes: Bytes = result.bytes().await?;
            table_bytes.push(bytes);
            
        }
        Ok(table_bytes)

    }

    /// If the user has permission to read the table, then this function returns a polars dataframe. 
    ///
    /// # Arguments
    ///
    /// * `table_name` - The fully qualified table name
    /// * `parallel_read` - true/false argument to read the table serially or in parallel
    ///
    /// # Examples
    ///
    /// ```
    /// let table_name: &str = "my_catalog.my_schema.my_table";
    /// let df = reader.read_delta_table_as_polars(table_path, true).await?;
    /// ```
    pub async fn read_delta_table_as_polars(&self, table_name: &str, parallel_read: bool) -> Result<PolarsDataFrame, Box<dyn Error>> {     
        // create empty DF - we will replace it later with the if/else
        let table_path: String = self.metastore_client.get_table(table_name).await?.storage_location.ok_or("Table Location Not Found.")?;
        let mut df: PolarsDataFrame = PolarsDataFrame::default();
        let mut table_bytes: Vec<Bytes> = Vec::default();

        if !permissions::can_read(self.api_client.clone(), &table_name, &self.principal).await? {
            log::info!("Permissions on Object {} Denied.", table_name);
            return Ok(df);
        } else {
            log::info!("Validated Permissions on Object: {}", table_name);
            // get the table as a vector of bytes each index is a parquet file 
            if parallel_read {
                log::info!("Parallel reading table.");
                table_bytes = self.parallel_read_table_as_bytes(&table_path).await?;
            } else {
                log::info!("Seirially readin table.");
                table_bytes = self.read_table_as_bytes(&table_path).await?;
            }
            
            // foreach file we need only the file name 
            // load the bytes into a polars dataframe
            for b in table_bytes {
                let cursor: Cursor<Bytes> = Cursor::new(b);
                let new_df: PolarsDataFrame = ParquetReader::new(cursor).finish()?;

                if df.is_empty() {
                    df = new_df.clone();
                } else {
                    df = match df.vstack(&new_df) {
                        Ok(stacked_df) => stacked_df,
                        Err(e) => {
                            // Handle the error if the vertical stack operation fails
                            log::error!("Error stacking DataFrames: {}", e);
                            df // Return the original DataFrame if the operation fails
                        }
                    };

                }
            }   
        }        
        Ok(df)
    }

    // doesn't work with cloud storage yet
    pub async fn read_delta_table_as_lazy_polars(&self, table_name: &str) -> Result<LazyFrame, Box<dyn Error>> {

        let table_path: String = self.metastore_client.get_table(table_name).await?.storage_location.ok_or("Table Location Not Found.")?;
        let table: DeltaTable = open_table_with_storage_options(table_path, self.storage_credentials.to_hash_map()).await?;

        let files: Vec<PathBuf> = table.get_file_uris()?.map(PathBuf::from).collect::<Vec<_>>(); 

        let df: LazyFrame = LazyFrame::scan_parquet_files(Arc::from(files), ScanArgsParquet::default())?;

        Ok(df)
    } 

    pub async fn write_polars_to_delta_table(&self, table_name: &str) -> Result<(), Box<dyn Error>> {     
        // create empty DF - we will replace it later with the if/else
        let table_path: String = self.metastore_client.get_table(table_name).await?.storage_location.ok_or("Table Location Not Found.")?;

        // INCOMPLETE

        Ok(())

    }



        /// If the user has permission to read the table, then this function returns a datafusion dataframe. 
    ///
    /// # Arguments
    ///
    /// * `table_name` - The fully qualified table name
    ///
    /// # Examples
    ///
    /// ```
    /// let table_name: &str = "my_catalog.my_schema.my_table";
    /// let df = reader.read_delta_table_as_datafusion(table_path).await;
    /// ```
    pub async fn write_datafusion_to_delta(&self, table_name: &str, df: DatafusionDataFrame) -> Result<(), Box<dyn Error>> {
        let table_metadata: Table = self.metastore_client.get_table(table_name).await?;


        if !permissions::can_write(self.api_client.clone(), &table_name, &self.principal).await? {
            log::error!("Permissions on Object {} Denied.", table_name);
            return Err(Box::<dyn Error>::from("Permission Denied."));
        } else {
            log::info!("Validated Permissions on Object: {}", table_name);

            log::info!("Reading Table: {}", table_metadata.full_name);
            let table: DeltaTable = open_table_with_storage_options(table_metadata.storage_location.ok_or("Table Location Not Found.")?, self.storage_credentials.to_hash_map()).await?;

            // let record_batches = df.collect().await?;
            // // get json rows
            // let json_rows = deltalake::datafusion::arrow::json::writer::record_batches_to_json_rows(&record_batches[..])?.read_to_string();

            // writer
            // .write(
            //     json_rows.lines()
            //         .map(|line| serde_json::from_str(line)?)
            //         .collect(),
            // )
            // .await?;

            // // Commit the changes to the table.
            // writer.flush_and_commit(&mut table).await?;
                
            
            return Ok(());
        }

        
    }



    // async fn polars_to_arrow(df: &polars::prelude::DataFrame) -> Result<RecordBatch, PolarsError> {

    //     let schema: SchemaRef = Arc::new(df.schema().clone());
    //     let arrays: Vec<arrow::array::ArrayRef> = df
    //         .columns()
    //         .iter()
    //         .map(|series| series.clone().to_arrow())
    //         .collect();
    //     RecordBatch::try_new(schema, arrays)?
    // }

    // async fn test(&self) -> Result<(), Box<dyn Error>> {
    //     let id_field = arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false);
    //     let schema = Arc::new(arrow::datatypes::Schema::new(vec![id_field]));
    //     let ids = arrow::array::Int32Array::from(vec![1, 2, 3, 4, 5]);
    //     let batch: RecordBatch = RecordBatch::try_new(schema, vec![Arc::new(ids)])?;
    //     let ops = DeltaOps::try_from_uri("../path/to/empty/dir").await?;
    //     let table = ops.write(vec![batch]).await?;

    //     Ok(())
    // }

    // async fn datafusion_to_arrow(&self, df: deltalake::datafusion::prelude::DataFrame) {
    //     // https://github.com/apache/datafusion/blob/e676f3c114ce00972b4bfb68c4e0a87e500a2286/datafusion-examples/examples/flight_server.rs#L102
    // }


}

