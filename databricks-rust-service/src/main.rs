#![warn(unused_extern_crates)]
use magic_crypt::{new_magic_crypt, MagicCryptTrait};
use log;
use dotenv::dotenv;
use std::env;
use serde_json::json;
use axum::http::StatusCode;
use axum::{
    routing::get, 
    Router,
    response::IntoResponse,
    Json,
};

use axum_extra::{
    TypedHeader,
    headers::{authorization::Bearer, Authorization}
};



#[tokio::main]
async fn main() {
    // load env vars and set logging 
    dotenv().ok();
    env_logger::builder()
    .filter_level(log::LevelFilter::Info)
    .init();

    // configure router and listener
    let app: Router = create_router();

    // run locally on port 8000 for now
    let port = env::var("PORT").unwrap_or_else(|_| "8000".to_string());
    let addr = format!("0.0.0.0:{}", port);

    log::info!("Running on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}


fn create_router() -> Router {
    // we only need a single endpoint
    Router::new()
        .route("/api/storage_credentials", get(get_storage_credentials))

}


async fn get_storage_credentials(TypedHeader(headers): TypedHeader<Authorization<Bearer>> ) -> impl IntoResponse { //Json(payload): Json<AuthPayload>) -> impl IntoResponse {

    // validate bearer token is valid
    let expected_token: String = env::var("API_TOKEN").expect("API_TOKEN not set");
    let auth_token = headers.token();

    log::info!("Validating Authorization. ");
    if auth_token != expected_token {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Unauthorized"})),
        ).into_response();
    }
    log::info!("Authorized.");

    let azure_storage_account_name: String =
    env::var("AZURE_STORAGE_ACCOUNT_NAME").expect("AZURE_STORAGE_ACCOUNT_NAME not set");
    let azure_client_id: String = env::var("AZURE_CLIENT_ID").expect("AZURE_CLIENT_ID not set");
    let azure_client_secret: String =
        env::var("AZURE_CLIENT_SECRET").expect("AZURE_CLIENT_SECRET not set");
    let azure_tenant_id: String = env::var("AZURE_TENANT_ID").expect("AZURE_TENANT_ID not set");

    // respond with encrypted secrets 
    let json_response = serde_json::json!({
        "azure_storage_account_name": encrypt_string(azure_storage_account_name, &expected_token),
        "azure_client_id": encrypt_string(azure_client_id, &expected_token),
        "azure_client_secret": encrypt_string(azure_client_secret, &expected_token),
        "azure_tenant_id": encrypt_string(azure_tenant_id, &expected_token),
    });

    Json(json_response).into_response()
    
}



fn encrypt_string(plain_text: String, encrypt_key: &str) -> String {
    let mc = new_magic_crypt!(encrypt_key, 256);
    let encrypted_text = mc.encrypt_str_to_base64(plain_text);
    encrypted_text
}