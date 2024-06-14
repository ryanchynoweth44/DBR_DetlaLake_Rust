# Project README

## Overview

This project for personal learning! 

This is a simple Rust application using Axum for creating an HTTP server. The server exposes a single endpoint that returns encrypted Azure storage credentials. Authorization is handled via Bearer tokens passed in the `Authorization` header.


## Installation

1. Ensure you have Rust installed. If not, install it from [rust-lang.org](https://www.rust-lang.org/).
2. Clone the repository
3. Create a `.env` file in the root directory with the following variables:
   ```env
   API_TOKEN=your_api_token
   AZURE_STORAGE_ACCOUNT_NAME=your_storage_account_name
   AZURE_CLIENT_ID=your_client_id
   AZURE_CLIENT_SECRET=your_client_secret
   AZURE_TENANT_ID=your_tenant_id
   PORT=8000  # or any other port you prefer
   ```

## Usage

1. Build and run the application:
   ```sh
    cd databricks-rust-service
   cargo run -p databricks_rust_service
   ```
2. The server will start on the port specified in the `.env` file (default is `8000`).
3. Access the endpoint at `http://localhost:8000/api/storage_credentials` with a valid Bearer token in the `Authorization` header.
