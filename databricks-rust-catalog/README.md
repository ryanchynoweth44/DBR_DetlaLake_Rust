# Databricks Rust Catalog

This catalog is an integration with Databricks' Unity Catalog. It allows users to interact and obtain the required metadata and to access the tables directly without using Databricks compute. The library is integrated with Unity Catalog permissions so users are only able to access the data objects which they have the appropriate permissions for. 

Please see `main.rs` for a code sample of the application. 


## Installation

1. Ensure you have Rust installed. If not, install it from [rust-lang.org](https://www.rust-lang.org/).
2. Clone the repository
3. Create a `.env` file in the root directory with the following variables:
    ```env
    USER_NAME=<username@email.com>
    WORKSPACE_NAME=adb-12345678912345.11.azuredatabricks.net
    DB_TOKEN=<token>
    STORAGE_PORT=8000  # or any other port you prefer
    STORAGE_URL=http://localhost
    ```

## Usage

1. Build and run the application:
    ```sh
    cd databricks-rust-catalog
    cargo run -p databricks_rust_catalog
    ```
2. The server will start on the port specified in the `.env` file (default is `8000`).
3. Access the endpoint at `http://localhost:8000/api/storage_credentials` with a valid Bearer token in the `Authorization` header.
