CREATE TABLE IF NOT EXISTS schemas (
    name TEXT,
    catalog_name TEXT,
    owner TEXT,
    comment TEXT,
    storage_root TEXT,
    enable_predictive_optimization TEXT,
    metastore_id TEXT,
    full_name TEXT,
    storage_location TEXT,
    created_at INTEGER,
    created_by TEXT,
    updated_at INTEGER,
    updated_by TEXT,
    catalog_type TEXT,
    browse_only BOOLEAN,
    schema_id TEXT--,
    -- FOREIGN KEY (catalog_name) REFERENCES Catalog(name)
);