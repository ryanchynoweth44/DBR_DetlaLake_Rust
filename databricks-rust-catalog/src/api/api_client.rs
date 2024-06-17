use log;
use reqwest::{header::HeaderMap, Error, Response};

/// Represents an API client for making HTTP requests.
#[derive(Clone)]
pub struct APIClient {
    /// The database token used for authorization.
    pub db_token: String,
    /// The name of the workspace associated with the API client.
    pub workspace_name: String,
}

impl APIClient {
    /// Fetches data from the specified URL asynchronously.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to fetch data from.
    ///
    /// # Returns
    ///
    /// A Result containing the HTTP response if successful, or an Error if an error occurs.
    pub async fn fetch(&self, url: &str, token: Option<&str>) -> Result<Response, Error> {
        let client: reqwest::Client = reqwest::Client::new();
        let mut headers: HeaderMap = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());

        // Use the provided token if it exists, otherwise use the default db_token
        let auth_token = token.unwrap_or(&self.db_token);
        headers.insert(
            "Authorization",
            format!("Bearer {}", auth_token).parse().unwrap(),
        );

        let response: Response = client.get(url).headers(headers.clone()).send().await?;

        // Check if the response status code is not 200
        if !response.status().is_success() {
            // Log an error message
            let error_response: Response = client.get(url).headers(headers.clone()).send().await?;

            let resp_text = error_response.text().await?;
            log::error!(
                "Request to {} failed with status code: {} - {}",
                url,
                response.status(),
                resp_text
            );
        }

        Ok(response)
    }
}
