use rabbitmesh_macros::{
    audit_log, cached, metrics, rate_limit, require_auth, require_permission,
    service_impl, service_method, transactional, validate
};
use rabbitmesh::Message;
use serde_json::Value;
use std::sync::Arc;

use crate::{
    handler::UserHandler,
    model::{CreateUserRequest, UpdateUserRequest, UserResponse},
};

static HANDLER: std::sync::OnceLock<Arc<UserHandler>> = std::sync::OnceLock::new();

pub struct UserService {}

#[service_impl]
impl UserService {
    pub fn new(handler: UserHandler) -> Self {
        let arc_handler = Arc::new(handler);
        let _ = HANDLER.set(arc_handler);
        Self {}
    }


    #[service_method("POST /users")]
    #[validate]
    #[rate_limit(10, 60)]
    #[metrics]
    #[audit_log]
    pub async fn create_user(msg: Message) -> Result<Value, String> {
        let request: CreateUserRequest = msg.deserialize_payload()
            .map_err(|e| format!("Invalid request format: {}", e))?;

        let handler = HANDLER.get().expect("Handler not initialized");
        
        match handler.create_user(request).await {
            Ok(user) => {
                tracing::info!("Created user: {}", user.id);
                Ok(serde_json::to_value(user).unwrap())
            },
            Err(e) => Err(format!("Failed to create user: {}", e))
        }
    }

    #[service_method("GET /users/:id")]
    #[require_auth]
    #[require_permission("users:read")]
    #[cached(300)]
    #[metrics]
    pub async fn get_user(msg: Message) -> Result<Value, String> {
        // Extract user_id from metadata (set by the RPC framework)
        let user_id = msg.metadata.get("user_id")
            .or_else(|| msg.metadata.get("id"))
            .ok_or("User ID not found in request")?;
        
        tracing::info!("Getting user: {}", user_id);
        let handler = HANDLER.get().expect("Handler not initialized");
        
        match handler.get_user(user_id).await {
            Ok(Some(user)) => Ok(serde_json::to_value(user).unwrap()),
            Ok(None) => Err("User not found".to_string()),
            Err(e) => Err(format!("Failed to get user: {}", e))
        }
    }

    #[service_method("PUT /users/:id")]
    #[require_auth]
    #[require_permission("users:write")]
    #[validate]
    #[rate_limit(20, 60)]
    #[transactional]
    #[metrics]
    #[audit_log]
    pub async fn update_user(msg: Message) -> Result<Value, String> {
        let request: UpdateUserRequest = msg.deserialize_payload()
            .map_err(|e| format!("Invalid request format: {}", e))?;
            
        let user_id = msg.metadata.get("id")
            .ok_or("User ID not found in path")?;

        let handler = HANDLER.get().expect("Handler not initialized");
        
        match handler.update_user(user_id, request).await {
            Ok(Some(user)) => Ok(serde_json::to_value(user).unwrap()),
            Ok(None) => Err("User not found".to_string()),
            Err(e) => Err(format!("Failed to update user: {}", e))
        }
    }

    #[service_method("DELETE /users/:id")]
    #[require_auth]
    #[require_permission("users:delete")]
    #[rate_limit(5, 60)]
    #[transactional]
    #[metrics]
    #[audit_log]
    pub async fn delete_user(msg: Message) -> Result<Value, String> {
        let user_id = msg.metadata.get("id")
            .ok_or("User ID not found in path")?;

        let handler = HANDLER.get().expect("Handler not initialized");
        
        match handler.delete_user(user_id).await {
            Ok(true) => {
                Ok(serde_json::json!({
                    "message": "User deleted successfully",
                    "deleted_at": chrono::Utc::now().to_rfc3339()
                }))
            },
            Ok(false) => Err("User not found".to_string()),
            Err(e) => Err(format!("Failed to delete user: {}", e))
        }
    }

    #[service_method("GET /users")]
    #[require_auth]
    #[require_permission("users:read")]
    #[cached(60)]
    #[rate_limit(50, 60)]
    #[metrics]
    pub async fn list_users(msg: Message) -> Result<Value, String> {
        let limit = msg.metadata.get("limit")
            .and_then(|l| l.parse::<i64>().ok())
            .unwrap_or(50);
        
        let skip = msg.metadata.get("skip")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        tracing::info!("Listing users with limit: {}, skip: {}", limit, skip);
        let handler = HANDLER.get().expect("Handler not initialized");

        match handler.list_users(limit, skip).await {
            Ok(users) => Ok(serde_json::to_value(users).unwrap()),
            Err(e) => Err(format!("Failed to list users: {}", e))
        }
    }

    #[service_method("GET /users/email/:email")]
    #[require_auth]
    #[require_permission("users:read")]
    #[cached(300)]
    #[metrics]
    pub async fn get_user_by_email(msg: Message) -> Result<Value, String> {
        let email = msg.metadata.get("email")
            .ok_or("Email not provided in request")?;

        tracing::info!("Getting user by email: {}", email);
        let handler = HANDLER.get().expect("Handler not initialized");

        match handler.get_user_by_email(email).await {
            Ok(Some(user)) => Ok(serde_json::to_value(user).unwrap()),
            Ok(None) => Err("User not found".to_string()),
            Err(e) => Err(format!("Failed to get user by email: {}", e))
        }
    }
}