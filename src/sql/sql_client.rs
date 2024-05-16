// https://github.com/launchbadge/sqlx/tree/main/examples/sqlite/todos
use log;
use sqlx::migrate::MigrateError;
use crate::api::metastore::CatalogResponse;
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
                Ok(_) => log::info!("Create db success"),
                Err(error) => panic!("error: {}", error),
            }
        }
        let pool: sqlx::Pool<Sqlite> = SqlitePool::connect(database_path).await?;

        Ok(Self { pool, migrations_path})
    }

    pub async fn execute_sql(&mut self, query: &str) -> Result<SqliteQueryResult, Error> {
        log::info!("Executing SQL: {}", query);
        let result: SqliteQueryResult = sqlx::query(query).execute(&self.pool).await?;
        log::info!("--------------- {:?}", result);
        Ok(result)
    
    }

    pub async fn run_migrations(&self) -> Result<(), MigrateError> {
        log::info!("-------------- Running Migrations | Path: {}", &self.migrations_path);
        let migrations = std::path::Path::new(&self.migrations_path);

        let migration_results = sqlx::migrate::Migrator::new(migrations)
            .await
            .unwrap()
            .run(&self.pool)
            .await;

        match migration_results {
            Ok(_) => log::info!("Migration success"),
            Err(error) => {
                panic!("error: {}", error);
            }
        }
    
        log::info!("migration: {:?}", migration_results);

        migration_results
    }

    
    pub async fn write_catalog(&self, catalog_response: CatalogResponse) -> Result<(), sqlx::Error> {
        for catalog in catalog_response.catalogs {
            let result: SqliteQueryResult = sqlx::query(
                "INSERT OR REPLACE INTO catalogs (name, owner, comment, storage_root, provider_name, share_name, enable_predictive_optimization, metastore_id, created_at, created_by, updated_at, updated_by, catalog_type, storage_location, isolation_mode, connection_name, full_name, securable_kind, securable_type, browse_only)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)")
                .bind(catalog.name)
                .bind(catalog.owner)
                .bind(catalog.comment)
                .bind(catalog.storage_root)
                .bind(catalog.provider_name)
                .bind(catalog.share_name)
                .bind(catalog.enable_predictive_optimization)
                .bind(catalog.metastore_id)
                .bind(catalog.created_at)
                .bind(catalog.created_by)
                .bind(catalog.updated_at)
                .bind(catalog.updated_by)
                .bind(catalog.catalog_type)
                .bind(catalog.storage_location)
                .bind(catalog.isolation_mode)
                .bind(catalog.connection_name)
                .bind(catalog.full_name)
                .bind(catalog.securable_kind)
                .bind(catalog.securable_type)
                .bind(catalog.browse_only)
                .execute(&self.pool).await?;
        }    

        Ok(())
    }
    

}

