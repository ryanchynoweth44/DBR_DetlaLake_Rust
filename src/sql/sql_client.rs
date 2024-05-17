// https://github.com/launchbadge/sqlx/tree/main/examples/sqlite/todos
use log;
use sqlx::migrate::MigrateError;
use crate::api::metastore::{CatalogResponse, SchemaResponse};
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

    pub async fn execute_sql(&self, query: &str) -> Result<SqliteQueryResult, Error> {
        log::info!("Executing SQL: {}", query);
        let result = sqlx::query(query).execute(&self.pool).await;
        match result {
            Ok(res) => {
                // log::info!("--------------- {:?}", res);
                Ok(res)
            },
            Err(err) => {
                log::error!("Error executing SQL query: {}", err);
                Err(err)
            }
        }
    
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

    
    pub async fn write_catalogs(&self, catalog_response: CatalogResponse) -> Result<(), sqlx::Error> {
        for catalog in catalog_response.catalogs {
            let qry: String = format!(
                "INSERT OR REPLACE INTO catalogs (name, owner, comment, storage_root, provider_name, share_name, enable_predictive_optimization, metastore_id, created_at, created_by, updated_at, updated_by, catalog_type, storage_location, isolation_mode, connection_name, full_name, securable_kind, securable_type, browse_only)
                    VALUES ({:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?})"
                ,catalog.name
                ,catalog.owner
                ,catalog.comment.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.storage_root.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.provider_name.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.share_name.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.enable_predictive_optimization.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.metastore_id
                ,catalog.created_at
                ,catalog.created_by
                ,catalog.updated_at.map_or("NULL".to_string(), |v| format!("{:?}", v))
                ,catalog.updated_by.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.catalog_type
                ,catalog.storage_location.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.isolation_mode.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.connection_name.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.full_name
                ,catalog.securable_kind.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.securable_type.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)).trim_matches('"')
                ,catalog.browse_only.map_or("NULL".to_string(), |b| format!("{:?}", b)),

            );
                        

            let _result: &SqliteQueryResult = &self.execute_sql(&qry).await?;
        }
        Ok(())
    }
    



    pub async fn write_schemas(&self, schema_response: SchemaResponse) -> Result<(), sqlx::Error> {
        for schema in schema_response.schemas {

            let qry: String = format!(
                "INSERT OR REPLACE INTO schemas (name, catalog_name, owner, comment, storage_root, enable_predictive_optimization, metastore_id, full_name, storage_location, created_at, created_by, updated_at, updated_by, catalog_type, browse_only, schema_id)
                 VALUES ({:?}, {:?}, {:?}, {}, {}, {}, {:?}, '{}', {}, {:?}, {:?}, {}, {}, {}, {:?}, {:?})",
                schema.name,
                schema.catalog_name,
                schema.owner,
                schema.comment.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)),
                schema.storage_root.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)),
                schema.enable_predictive_optimization.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)),
                schema.metastore_id,
                schema.full_name,
                schema.storage_location.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)),
                schema.created_at,
                schema.created_by,
                schema.updated_at.map_or("NULL".to_string(), |v| format!("{:?}", v)),
                schema.updated_by.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)),
                schema.catalog_type.as_deref().map_or("NULL".to_string(), |s| format!("{:?}", s)),
                schema.browse_only.map_or("NULL".to_string(), |b| format!("{:?}", b)),
                schema.schema_id
            );
                        

            let _result: &SqliteQueryResult = &self.execute_sql(&qry).await?;


        }    
        Ok(())
    }

}

