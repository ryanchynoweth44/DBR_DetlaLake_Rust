use reqwest::{header::HeaderMap, Response, Error};
use log;

#[derive(Clone)]
pub struct APIClient {
    pub db_token: String,
    pub workspace_name: String,
}

impl APIClient {

    pub async fn fetch(&self, url: String) -> Result<Response, Error> {
        let client: reqwest::Client = reqwest::Client::new();
        let mut headers: HeaderMap = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("Authorization", format!("Bearer {}", &self.db_token).parse().unwrap());
        

        let response: Response = client.get(&url)
        .headers(headers.clone())
        .send()
        .await?;


        // Check if the response status code is not 200
        if !response.status().is_success() {
            // Log an error message
            log::error!("Request to {} failed with status code: {}", url, response.status());
        }
       

        Ok(response)
    }
}