// https://github.com/launchbadge/sqlx/tree/main/examples/sqlite/todos

use sqlx::migrate::MigrateError;
// use super::metastore::CatalogResponse;
use sqlx::{migrate::MigrateDatabase, Error, Sqlite};
use sqlx::sqlite::{SqliteQueryResult, SqlitePool};

pub struct SqlClient {
    pub pool: sqlx::Pool<Sqlite>,
    pub migrations_path: String,

}

impl SqlClient {
    pub async fn new(database_path: &str, migrations_path: String) -> Result<Self, Error> {
        // Create SQLite connection options
        if !Sqlite::database_exists(database_path).await? {
            // Sqlite::create_database(database_path).await?;
            match Sqlite::create_database(database_path).await {
                Ok(_) => println!("Create db success"),
                Err(error) => panic!("error: {}", error),
            }
        }
        let pool: sqlx::Pool<Sqlite> = SqlitePool::connect(database_path).await?;

        Ok(Self { pool, migrations_path})
    }

    pub async fn execute_sql(&mut self, query: &str) -> Result<(SqliteQueryResult), Error> {
        println!("Executing SQL: {}", query);
        let result: SqliteQueryResult = sqlx::query(query).execute(&self.pool).await?;
        println!("--------------- {:?}", result);
        Ok((result))
    
    }

    pub async fn run_migrations(&self) -> Result<(), MigrateError> {
        println!("-------------- Running Migrations | Path: {}", &self.migrations_path);
        let migrations = std::path::Path::new(&self.migrations_path);

        let migration_results = sqlx::migrate::Migrator::new(migrations)
            .await
            .unwrap()
            .run(&self.pool)
            .await;

        match migration_results {
            Ok(_) => println!("Migration success"),
            Err(error) => {
                panic!("error: {}", error);
            }
        }
    
        println!("migration: {:?}", migration_results);

        migration_results
    }



    pub async fn load_sql_file(&self, file_path: &str) -> Result<String, std::io::Error> {
        let sql_string = std::fs::read_to_string(file_path)? ;
        // println!("{:?}", sql_string);
        Ok(sql_string)
    }

    pub async fn execute_sql_files_from_directory(&mut self, directory_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Read all files from the directory
        let paths = std::fs::read_dir(directory_path)?;

        for path in paths {
            let entry = path?;
            let full_path = entry.path();
            let full_path_str = full_path.to_string_lossy();
            let file_contents = self.load_sql_file(&full_path_str).await?;
            self.execute_sql(&file_contents);
        }


        Ok(())
    }
    
    // this will not work
    pub fn compose_upsert_sql(&self, table_name: &str, merge_column: &str) -> String {
        
        format!(
            "INSERT INTO {table} *
            ON CONFLICT({id_column}) DO UPDATE SET * ",
            table = table_name,
            id_column = merge_column,
        )
    }

    pub async fn execute_upsert_sql(&mut self, table_name: &str, id_column: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Compose the upsert SQL query
        let upsert_sql: String = self.compose_upsert_sql(table_name, id_column);

        // Execute the SQL query
        self.execute_sql(&upsert_sql).await?;
        

        Ok(())
    }

    pub async fn create_catalogs_table(&mut self) -> Result<(), Error> {
        self.execute_sql(
            "CREATE TABLE IF NOT EXISTS catalogs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                owner TEXT NOT NULL,
                comment TEXT,
                storage_root TEXT,
                provider_name TEXT,
                share_name TEXT,
                enable_predictive_optimization TEXT,
                metastore_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                created_by TEXT NOT NULL,
                updated_at INTEGER,
                updated_by TEXT,
                catalog_type TEXT,
                storage_location TEXT,
                isolation_mode TEXT,
                connection_name TEXT,
                full_name TEXT,
                securable_kind TEXT,
                securable_type TEXT,
                browse_only BOOLEAN
            )"
        ).await?;
        Ok(())
    }
}

