use reqwest::{header::HeaderMap, Response, Error};


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

        Ok(response)
    }
}