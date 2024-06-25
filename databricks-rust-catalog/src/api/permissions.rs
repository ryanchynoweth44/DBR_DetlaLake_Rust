use super::api_client::APIClient;
use reqwest::{Error, Response};
use serde::Deserialize;

/// Returns User Struct containing the princpal used for authentication.
///
/// # Arguments
///
/// * `user_name` - The username associated to the token
/// * `user_token` - The token used for authentication against Unity Catalog
///
/// # Examples
///
/// ```ignore
/// let active_user: api::permissions::User = permissions_client.authenticate_user("ryan.chynoweth@databricks.com", db_token, workspace_name).await.unwrap();
/// ```
pub async fn authenticate_user(api_client: APIClient, user_name: &str) -> Result<bool, Error> {
    // need to add encryption and verification of user

    // user_token will likely be required in the future as there will be a service token and a user token.
    let auth_url: String = format!(
        "https://{}/api/2.0/preview/scim/v2/Me",
        api_client.workspace_name
    );

    let response: Response = api_client.fetch(&auth_url, None).await?;
    let status: bool = response.status().is_success();

    let user: User = response.json().await?;

    // need to properly authenticate that the user matches and the workspace matches. I should call the API using the provided token in the function

    if !status && user.user_name == user_name {
        log::error!("Failed to authenticate user: {}", user.user_name);
    } else {
        log::info!("User {} authentication was successful.", user.user_name);
    }

    Ok(status)
}

// /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}
/// Fetches permissions for a given object from Unity Catalog API.
///
/// # Arguments
///
/// * `api_client` - API client object for making HTTP requests.
/// * `securable_type` - Type of securable object (e.g., Catalog, Schema, Table).
/// * `full_name` - Fully qualified name of the object.
/// * `principal` - The principal user or group for which permissions are being fetched.
///
/// # Returns
///
/// * `PrivilegeAssignmentsResponse` - Response containing the permissions assignments for the object.
///
/// # Errors
///
/// Returns an `Error` if the API request fails or if the response cannot be parsed.
async fn fetch_permissions(
    api_client: APIClient,
    securable_type: SecurableType,
    full_name: &str,
    principal: &str,
) -> Result<PrivilegeAssignmentsResponse, Error> {
    // Determine the string representation of the securable type
    let securable_type_str = securable_type.to_string();

    // Split full name and make API calls to fetch permissions for each part
    let name_parts: Vec<&str> = full_name.split('.').collect();
    let catalog_name = name_parts.get(0).unwrap_or(&"").trim_matches('"'); // Always expect a catalog
    let schema_name = match (name_parts.get(0), name_parts.get(1)) {
        (Some(part1), Some(part2)) => format!("{}.{}", part1, part2),
        _ => "".to_string(), // Handle case where parts are missing
    };

    let mut privileges: PrivilegeAssignmentsResponse = PrivilegeAssignmentsResponse::new();

    // Fetch permissions for catalog
    if !name_parts.get(0).is_none() {
        let catalog_auth_url: String = format!(
            "https://{}/api/2.1/unity-catalog/permissions/{}/{}?principal={}",
            api_client.workspace_name, "catalog", catalog_name, principal
        );
        log::info!("Getting Catalog Permissions - {}", catalog_auth_url);
        let catalog_response: Response = api_client.fetch(&catalog_auth_url, None).await?;
        let catalog_perms: PrivilegeAssignmentsResponse = catalog_response.json().await?;
        privileges.add_assignment(catalog_perms, &catalog_name, SecurableType::Catalog);
    }

    // Fetch permissions for schema
    if !name_parts.get(1).is_none() {
        let schema_auth_url: String = format!(
            "https://{}/api/2.1/unity-catalog/permissions/{}/{}?principal={}",
            api_client.workspace_name, "schema", schema_name, principal
        );
        log::info!("Getting Schema Permissions - '{}'", schema_auth_url);
        let schema_response: Response = api_client.fetch(&schema_auth_url, None).await?;
        let schema_perms: PrivilegeAssignmentsResponse = schema_response.json().await?;
        privileges.add_assignment(schema_perms, &schema_name, SecurableType::Schema);
    }

    // Fetch permissions for the object itself
    if !name_parts.get(2).is_none() {
        let obj_auth_url: String = format!(
            "https://{}/api/2.1/unity-catalog/permissions/{}/{}?principal={}",
            api_client.workspace_name, &securable_type_str, full_name, principal
        );
        log::info!("Getting Object Permissions - {}", obj_auth_url);
        let obj_response: Response = api_client.fetch(&obj_auth_url, None).await?;
        let obj_perms: PrivilegeAssignmentsResponse = obj_response.json().await?;
        privileges.add_assignment(obj_perms, &full_name, SecurableType::Table);
    }

    Ok(privileges)
}

/// Fetches the owner of a specified object from Unity Catalog API.
/// Reference: /api/2.1/unity-catalog/{securable_type}/{full_name}
/// # Arguments
///
/// * `api_client` - API client object for making HTTP requests.
/// * `securable_type` - Type of securable object (e.g., Catalog, Schema, Table).
/// * `full_name` - Fully qualified name of the object.
///
/// # Returns
///
/// * `ObjectOwnerResponse` - Response containing the owner information for the object.
///
/// # Errors
///
/// Returns an `Error` if the API request fails or if the response cannot be parsed.
async fn get_object_owner(
    api_client: APIClient,
    securable_type: SecurableType,
    full_name: &str,
) -> Result<ObjectOwnerResponse, Error> {
    log::info!(
        "Checking ownership on {}: {}",
        securable_type.to_string(),
        full_name
    );

    let url: String = format!(
        "https://{}/api/2.1/unity-catalog/{}s/{}",
        api_client.workspace_name,
        securable_type.to_string(),
        full_name
    );
    let response: Response = api_client.fetch(&url, None).await?;
    let status: bool = response.status().is_success();
    let owner_response: ObjectOwnerResponse = response.json().await?;

    if !status {
        log::error!("Failed to get owner of object - {}", full_name);
    }

    Ok(owner_response)
}

/// Checks if a principal has the specified permissions on a given object.
///
/// # Arguments
///
/// * `api_client` - API client object for making HTTP requests.
/// * `securable_type` - Type of securable object (e.g., Catalog, Schema, Table).
/// * `full_name` - Fully qualified name of the object.
/// * `principal` - The principal user or group for which permissions are being checked.
/// * `permissions` - Vector of permissions to check against.
///
/// # Returns
///
/// * `bool` - `true` if the principal has the specified permissions, `false` otherwise.
///
/// # Errors
///
/// Returns an `Error` if the API request fails or if the response cannot be parsed.
async fn check_permissions(
    api_client: APIClient,
    securable_type: SecurableType,
    full_name: &str,
    principal: &str,
    permissions: Vec<&str>,
) -> Result<bool, Error> {
    let mut perm_check: bool = false; // deny by default
    let object_permissions: PrivilegeAssignmentsResponse = fetch_permissions(
        api_client.clone(),
        securable_type.clone(),
        full_name,
        principal,
    )
    .await?;

    // split full name and make 3 different api calls since permissions can be delagated
    let name_parts: Vec<&str> = full_name.split('.').collect();
    let catalog_name = name_parts.get(0).unwrap().trim_matches('"'); // always expect a catalog   - ok_or("Error Getting Catalog")?
    let schema_name = match (name_parts.get(0), name_parts.get(1)) {
        // may not always be a schema
        (Some(part1), Some(part2)) => format!("{}.{}", part1, part2),
        _ => "".to_string(), // handle case where parts are missing
    };

    // if they are an owner of the object or one of the parent objects then we return TRUE
    // start with the object and go higher b/c its more likely users are owners of tables than catalogs
    if get_object_owner(api_client.clone(), securable_type.clone(), full_name)
        .await?
        .owner
        == principal
    {
        log::info!("Princpal {} is an owner of {}. ", principal, full_name);
        perm_check = true;
    } else if !name_parts.get(1).is_none()
        && get_object_owner(api_client.clone(), SecurableType::Schema, &schema_name)
            .await?
            .owner
            == principal
    {
        // if princpal is the owner of the schema then return True
        log::info!("Princpal {} is an owner of {}. ", principal, schema_name);
        perm_check = true;
    } else if !name_parts.get(0).is_none()
        && get_object_owner(api_client.clone(), SecurableType::Catalog, &catalog_name)
            .await?
            .owner
            == principal
    {
        // if princpal is the owner of the catalog then return True
        log::info!("Princpal {} is an owner of {}. ", principal, catalog_name);
        perm_check = true;
    }
    // if there are permissions on the object to review
    else if let Some(assigns) = object_permissions.privilege_assignments {
        log::info!(
            "Princpal {} not an owner of {} or any parent object. ",
            principal,
            full_name
        );
        for s in assigns {
            // we do not need to verify principal as we only get permissions for that principal
            // but do want to log the princpal we are mapping the current user to i.e. if they are part of a group
            if let Some(p) = &s.principal {
                if let Some(pp) = s.privileges {
                    for value in pp {
                        // if the value is in the read_list vec then return TRUE
                        if permissions.contains(&value.as_str()) {
                            log::info!(
                                "Principal {} has {} permissions on {}.",
                                p,
                                value,
                                s.object_name
                            );
                            perm_check = true;
                        }
                    }
                }
            }
        }
    }
    Ok(perm_check)
}

/// Checks if a principal can read a given object from Unity Catalog API.
///
/// # Arguments
///
/// * `api_client` - API client object for making HTTP requests.
/// * `full_name` - Fully qualified name of the object.
/// * `principal` - The principal user or group for which permissions are being checked.
///
/// # Returns
///
/// * `bool` - `true` if the principal can read the object, `false` otherwise.
///
/// # Errors
///
/// Returns an `Error` if the API request fails or if the response cannot be parsed.
pub async fn can_read(
    api_client: APIClient,
    full_name: &str,
    principal: &str,
) -> Result<bool, Error> {
    let readable_permissions = vec!["SELECT", "ALL_PRIVILEGES"];
    let securable_type: SecurableType = SecurableType::Table;

    // split full name and make 3 different api calls since permissions can be delagated
    let name_parts: Vec<&str> = full_name.split('.').collect();
    let catalog_name = name_parts.get(0).unwrap().trim_matches('"'); // always expect a catalog
    let schema_name = match (name_parts.get(0), name_parts.get(1)) {
        // may not always be a schema
        (Some(part1), Some(part2)) => format!("{}.{}", part1, part2),
        _ => "".to_string(), // handle case where parts are missing
    };
    log::info!(
        "Checking if {} can read the following objects: {} | {} | {}",
        principal,
        catalog_name,
        schema_name,
        full_name
    );

    let readable: bool = check_permissions(
        api_client,
        securable_type,
        full_name,
        principal,
        readable_permissions,
    )
    .await?; // deny by default

    Ok(readable)
}

/// Checks if a principal can modify a given object from Unity Catalog API.
///
/// # Arguments
///
/// * `api_client` - API client object for making HTTP requests.
/// * `full_name` - Fully qualified name of the object.
/// * `principal` - The principal user or group for which permissions are being checked.
///
/// # Returns
///
/// * `bool` - `true` if the principal can read the object, `false` otherwise.
///
/// # Errors
///
/// Returns an `Error` if the API request fails or if the response cannot be parsed.

pub async fn can_write(
    api_client: APIClient,
    full_name: &str,
    principal: &str,
) -> Result<bool, Error> {
    let writable_permissions: Vec<&str> = vec!["MODIFY", "ALL_PRIVILEGES"];
    let securable_type: SecurableType = SecurableType::Table;

    // split full name and make 3 different api calls since permissions can be delagated
    let name_parts: Vec<&str> = full_name.split('.').collect();
    let catalog_name = name_parts.get(0).unwrap().trim_matches('"'); // always expect a catalog
    let schema_name = match (name_parts.get(0), name_parts.get(1)) {
        // may not always be a schema
        (Some(part1), Some(part2)) => format!("{}.{}", part1, part2),
        _ => "".to_string(), // handle case where parts are missing
    };
    log::info!(
        "Checking if {} can read the following objects: {} | {} | {}",
        principal,
        catalog_name,
        schema_name,
        full_name
    );

    let writable: bool = check_permissions(
        api_client,
        securable_type,
        full_name,
        principal,
        writable_permissions,
    )
    .await?; // deny by default

    Ok(writable)
}

// wrapper struct to hold all permissions on an object
#[derive(Debug, Deserialize, Clone)]
pub struct PrivilegeAssignmentsResponse {
    pub privilege_assignments: Option<Vec<PrivilegeAssignment>>,
}
impl PrivilegeAssignmentsResponse {
    // Constructor to create an empty PrivilegeAssignmentsResponse
    pub fn new() -> Self {
        PrivilegeAssignmentsResponse {
            privilege_assignments: Some(Vec::new()),
        }
    }

    // Method to extend privilege_assignments vector
    // object type needs to be "securable type"
    pub fn add_assignment(
        &mut self,
        privilege_assignments: PrivilegeAssignmentsResponse,
        object_name: &str,
        object_type: SecurableType,
    ) {
        if let Some(ref mut self_privs) = self.privilege_assignments {
            if let Some(mut privs) = privilege_assignments.privilege_assignments {
                for assignment in &mut privs {
                    assignment.object_name = object_name.to_string();
                    assignment.object_type = Some(object_type.clone());
                }
                self_privs.extend(privs);
            }
        }
    }
}

// struct to old ownership information
#[derive(Debug, Deserialize, Clone)]
pub struct ObjectOwnerResponse {
    pub full_name: String,
    pub owner: String,
}

// Struct to represent the privilege assignment objects in UC.
#[derive(Debug, Deserialize, Clone)]
pub struct PrivilegeAssignment {
    #[serde(skip)]
    pub object_name: String,
    pub object_type: Option<SecurableType>,
    pub principal: Option<String>,
    pub privileges: Option<Vec<String>>,
}

// A struct to represent the user object returned by the authentication endpoint
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: String,
    pub user_name: String,
    pub display_name: String,
    pub active: bool,
}

/// Enum representing various Unity Catalog (UC) securable objects
/// Refer to: https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html
#[derive(Debug, Deserialize, Clone)]
pub enum SecurableType {
    Catalog,           // metastore ownership
    Schema,            // catalog ownership
    Table,             // schema ownership
    StorageCredential, // metastore ownership
    ExternalLocation,  // metastore ownership
    Function,          // schema ownership
    Share,             // metastore ownership
    Provider,          // metastore ownership
    Recipient,         // metastore ownership
    Metastore, // account ownership - we can likely disregard as we are only working with single metastores
    Volume,    // schema ownership
    Connection, // federation - metastore ownership
}
impl std::str::FromStr for SecurableType {
    type Err = ();
    /// Converts a string slice to a `SecurableType` enum
    /// Returns an error if the input string does not match any variant
    fn from_str(input: &str) -> Result<SecurableType, Self::Err> {
        match input {
            "catalog" => Ok(SecurableType::Catalog),
            "schema" => Ok(SecurableType::Schema),
            "table" => Ok(SecurableType::Table),
            "storage_credential" => Ok(SecurableType::StorageCredential),
            "external_location" => Ok(SecurableType::ExternalLocation),
            "function" => Ok(SecurableType::Function),
            "share" => Ok(SecurableType::Share),
            "provider" => Ok(SecurableType::Provider),
            "recipient" => Ok(SecurableType::Recipient),
            "metastore" => Ok(SecurableType::Metastore),
            "volume" => Ok(SecurableType::Volume),
            "connection" => Ok(SecurableType::Connection),
            _ => Err(()),
        }
    }
}
impl ToString for SecurableType {
    /// Converts a `SecurableType` enum to a string
    fn to_string(&self) -> String {
        match self {
            SecurableType::Catalog => "catalog".to_string(),
            SecurableType::Schema => "schema".to_string(),
            SecurableType::Table => "table".to_string(),
            SecurableType::StorageCredential => "storage_credential".to_string(),
            SecurableType::ExternalLocation => "external_location".to_string(),
            SecurableType::Function => "function".to_string(),
            SecurableType::Share => "share".to_string(),
            SecurableType::Provider => "provider".to_string(),
            SecurableType::Recipient => "recipient".to_string(),
            SecurableType::Metastore => "metastore".to_string(),
            SecurableType::Volume => "volume".to_string(),
            SecurableType::Connection => "connection".to_string(),
        }
    }
}
