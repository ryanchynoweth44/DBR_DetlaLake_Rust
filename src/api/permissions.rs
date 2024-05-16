// use reqwest::{Response, Error};
// use super::api_client::APIClient;


// pub struct Permissions {
//     pub api_client: APIClient,
// }

// impl Permissions {

//     async fn authenticate_user(&self, user_name: String, user_token: String) -> Result<Response, Error> {
//         let auth_url: String = format!("https://{}/api/2.0/preview/scim/v2/Me", &self.api_client.workspace_name);

//         let response: Response = self.api_client.fetch(auth_url).await?;

//         // let catalogs: CatalogResponse = response.json().await?;
        
//         // UNFINISHED 
//         // ASSERT THAT USER NAME IS VALID FOR THE GIVEN TOKEN
//         // THIS IS USED TO VERIFY UC PERMISSIONS FOR READ/WRITE

        
//         Ok(response)
//     }

//     async fn fetch_permissions(&self, securable_type: String, full_name: String) -> Result<Response, Error> {
//         let auth_url: String = format!("https://{}/api/2.1/unity-catalog/permissions/{}/{}", &self.api_client.workspace_name, securable_type, full_name);

//         let response: Response = self.api_client.fetch(auth_url).await?;

//         // let catalogs: PrivilegeResponse = response.json().await?;
        
//         // UNFINISHED 
//         // {
//         //   "privilege_assignments": [
//         //     {
//         //       "principal": "string",
//         //       "privileges": [
//         //         "SELECT"
//         //       ]
//         //     }
//         //   ]
//         // }
        
        
//         Ok(response)
//     }

//     // PLACEHOLDER
//     async fn can_read(&self, privilege_response: PrivilegeResponse) -> bool {
//         let mut readable = false; // deny by default

//         readable
//     }

//     // PLACEHOLDER
//     async fn can_write(&self, privilege_response: PrivilegeResponse) -> bool {
//         let mut writable = false; // deny by default

//         writable
//     }
// }

// // PLACEHOLDER
// struct PrivilegeResponse {
//     privilege_assignments: String,
// }