# Databricks - Delta Lake - Rust

This repository is purposed for running rust workloads against Databricks Datasets. It will include the collection of data from the Databricks [REST API](https://docs.databricks.com/api/workspace/catalogs/list), reading data directly from storage using the Unity Catalog APIs for metadata, and a UI for exploring datasets. My hope is to integrate Rust and Polars/Spark with Databricks datasets. 

NOT FOR PRODUCTION USE!

## Resources: 
- [Install and Learn Rust](https://www.rust-lang.org/learn)
- [Rust Book](https://doc.rust-lang.org/book/)
- VS Code Extension - [rust-analyzer](https://rust-analyzer.github.io/)
- Documentation generation: `cargo doc --open --no-deps --target-dir=./docs`
- [Integration Tests](https://doc.rust-lang.org/book/ch11-03-test-organization.html) and [Test Selection](https://doc.rust-lang.org/cargo/commands/cargo-test.html)
- `cargo run -p databricks_rust_catalog`
- [Twilio Blog](https://www.twilio.com/en-us/blog/build-high-performance-rest-apis-rust-axum)