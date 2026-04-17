use axum::{
    extract::{Path, Query, State},
    http::{StatusCode},
    response::{Json, Response},
    routing::{get, post, put, delete},
    Router,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use rabbitmesh::{ServiceClient, RabbitMeshError};
use crate::registry::ServiceRegistry;

/// Gateway state containing the RabbitMQ client and service registry
#[derive(Debug, Clone)]
pub struct GatewayState {
    /// Client for calling microservices via RabbitMQ
    pub service_client: Arc<ServiceClient>,
    /// Registry of available services and their methods
    pub service_registry: Arc<ServiceRegistry>,
}

/// Create the auto-router that handles all REST API calls
/// 
/// ## How it works:
/// 1. Frontend sends HTTP request to gateway
/// 2. Gateway parses the request and extracts service/method  
/// 3. Gateway calls microservice via RabbitMQ (NO PORTS!)
/// 4. Microservice processes request and responds via RabbitMQ
/// 5. Gateway returns HTTP response to frontend
/// 
/// Example flow:
/// ```text
/// Frontend -> GET /api/v1/user-service/users/123
///          -> Gateway extracts: service="user-service", method="get_user", params={id: 123}
///          -> Gateway calls service via RabbitMQ
///          -> user-service processes request 
///          -> user-service responds via RabbitMQ
///          -> Gateway returns JSON to frontend
/// ```
pub async fn create_auto_router(amqp_url: impl Into<String>) -> Result<Router, RabbitMeshError> {
    info!("🌐 Creating auto-router gateway");
    
    // Create service client that talks to microservices via RabbitMQ
    let service_client = Arc::new(
        ServiceClient::new("api-gateway", amqp_url).await?
    );
    
    let service_registry = Arc::new(ServiceRegistry::new());
    
    let state = GatewayState {
        service_client,
        service_registry,
    };

    let router = Router::new()
        // Health check endpoint
        .route("/health", get(health_check))
        .route("/health/{service}", get(service_health_check))
        
        // Auto-generated REST API routes
        // Pattern: /api/v1/{service}/{method}/{params...}
        .route("/api/v1/{service}/{method}", post(handle_rpc_call))
        .route("/api/v1/{service}/{method}", get(handle_rpc_call))
        .route("/api/v1/{service}/{method}", put(handle_rpc_call))
        .route("/api/v1/{service}/{method}", delete(handle_rpc_call))
        
        // Parameterized routes: /api/v1/user-service/users/123
        .route("/api/v1/{service}/{method}/{param}", get(handle_rpc_call_with_param))
        .route("/api/v1/{service}/{method}/{param}", post(handle_rpc_call_with_param))
        .route("/api/v1/{service}/{method}/{param}", put(handle_rpc_call_with_param))
        .route("/api/v1/{service}/{method}/{param}", delete(handle_rpc_call_with_param))
        
        // Service registry endpoints
        .route("/registry/services", get(list_services))
        .route("/registry/services/{service}", get(describe_service))
        
        // Client generator compatible endpoint
        .route("/api/services", get(list_services_for_client_generator))
        
        .with_state(state);

    info!("✅ Auto-router created, ready to proxy HTTP -> RabbitMQ -> Microservices");
    Ok(router)
}

/// Handle RPC call without path parameters
/// 
/// Examples:
/// - POST /api/v1/user-service/create_user  
/// - GET /api/v1/product-service/list_products
async fn handle_rpc_call(
    State(state): State<GatewayState>,
    Path((service, method)): Path<(String, String)>,
    Query(query_params): Query<HashMap<String, String>>,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, GatewayError> {
    debug!("🔄 RPC call: {}.{}", service, method);
    
    // Prepare parameters from query params and body
    let params = prepare_params(query_params, body);
    
    // In a real implementation, you'd extract headers here
    let params_with_meta = params;
    
    // Call microservice via RabbitMQ (THIS IS THE MAGIC!)
    let response = state
        .service_client
        .call_with_timeout(&service, &method, params_with_meta, Duration::from_secs(30))
        .await?;
    
    // Convert RPC response to HTTP response
    match response {
        rabbitmesh::message::RpcResponse::Success { data, processing_time_ms } => {
            debug!("✅ RPC success: {}.{} ({}ms)", service, method, processing_time_ms);
            Ok(Json(data))
        }
        rabbitmesh::message::RpcResponse::Error { error, code, details } => {
            warn!("❌ RPC error: {}.{} - {}", service, method, error);
            Err(GatewayError::ServiceError {
                service: service.clone(),
                method: method.clone(),
                error,
                code,
                details,
            })
        }
    }
}

/// Handle RPC call with single path parameter
/// 
/// Examples:
/// - GET /api/v1/user-service/get_user/123 -> get_user(user_id: 123)
/// - DELETE /api/v1/order-service/cancel_order/abc-456 -> cancel_order(order_id: "abc-456")
async fn handle_rpc_call_with_param(
    State(state): State<GatewayState>,
    Path((service, method, param)): Path<(String, String, String)>,
    Query(query_params): Query<HashMap<String, String>>,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, GatewayError> {
    debug!("🔄 RPC call with param: {}.{}({})", service, method, param);
    
    // Add path parameter to query params
    let mut all_params = query_params;
    
    // Try to parse param as different types
    let param_value = if let Ok(num) = param.parse::<i64>() {
        Value::Number(serde_json::Number::from(num))
    } else if let Ok(num) = param.parse::<f64>() {
        Value::Number(serde_json::Number::from_f64(num).unwrap_or_else(|| serde_json::Number::from(0)))
    } else if param == "true" || param == "false" {
        Value::Bool(param == "true")
    } else {
        Value::String(param)
    };
    
    // Common parameter names based on method patterns
    let param_key = match method.as_str() {
        m if m.contains("user") => "user_id",
        m if m.contains("order") => "order_id", 
        m if m.contains("product") => "product_id",
        _ => "id", // Default fallback
    };
    
    all_params.insert(param_key.to_string(), param_value.to_string());
    
    let params = prepare_params(all_params, body);
    let params_with_meta = params;
    
    // Call microservice via RabbitMQ
    let response = state
        .service_client
        .call_with_timeout(&service, &method, params_with_meta, Duration::from_secs(30))
        .await?;
    
    match response {
        rabbitmesh::message::RpcResponse::Success { data, processing_time_ms } => {
            debug!("✅ RPC success: {}.{} ({}ms)", service, method, processing_time_ms);
            Ok(Json(data))
        }
        rabbitmesh::message::RpcResponse::Error { error, code, details } => {
            warn!("❌ RPC error: {}.{} - {}", service, method, error);
            Err(GatewayError::ServiceError {
                service,
                method,
                error,
                code,
                details,
            })
        }
    }
}

/// Prepare parameters from query params and request body
fn prepare_params(
    query_params: HashMap<String, String>,
    body: Option<Json<Value>>,
) -> Value {
    let mut params = serde_json::Map::new();
    
    // Add query parameters
    for (key, value) in query_params {
        // Try to parse value as JSON, fallback to string
        let parsed_value = serde_json::from_str(&value)
            .unwrap_or_else(|_| Value::String(value));
        params.insert(key, parsed_value);
    }
    
    // Add body parameters
    if let Some(Json(body_value)) = body {
        if let Value::Object(body_map) = body_value {
            for (key, value) in body_map {
                params.insert(key, value);
            }
        }
    }
    
    Value::Object(params)
}


/// Health check for the gateway itself
async fn health_check(State(state): State<GatewayState>) -> Result<Json<Value>, GatewayError> {
    let is_healthy = state.service_client.is_healthy().await;
    let stats = state.service_client.get_stats().await;
    
    Ok(Json(serde_json::json!({
        "status": if is_healthy { "healthy" } else { "unhealthy" },
        "gateway": "rabbitmesh-gateway",
        "version": env!("CARGO_PKG_VERSION"),
        "connection": stats.connection_stats,
        "rpc": stats.rpc_stats
    })))
}

/// Health check for specific service
async fn service_health_check(
    State(state): State<GatewayState>,
    Path(service): Path<String>,
) -> Result<Json<Value>, GatewayError> {
    // Try to ping the service
    match state.service_client.call_with_timeout(
        &service,
        "ping",
        serde_json::json!({}),
        Duration::from_secs(5)
    ).await {
        Ok(_) => Ok(Json(serde_json::json!({
            "service": service,
            "status": "healthy"
        }))),
        Err(_) => Ok(Json(serde_json::json!({
            "service": service,
            "status": "unhealthy"
        })))
    }
}

/// List all registered services by discovering them via RabbitMQ
async fn list_services(State(state): State<GatewayState>) -> Json<Value> {
    let mut discovered_services = Vec::new();
    
    // Dynamically discover services from RabbitMQ queues
    let discovered_service_names = discover_services_from_rabbitmq().await;
    
    for service_name in discovered_service_names {
        // Try to ping each service to see if it's available
        match state.service_client.call_with_timeout(
            &service_name,
            "ping",
            serde_json::json!({}),
            Duration::from_secs(2)
        ).await {
            Ok(_) => {
                discovered_services.push(serde_json::json!({
                    "name": service_name,
                    "status": "healthy",
                    "discovered_via": "rabbitmq_queue_discovery"
                }));
            }
            Err(_) => {
                // Service not responding, but queue exists
                discovered_services.push(serde_json::json!({
                    "name": service_name,
                    "status": "unreachable",
                    "discovered_via": "rabbitmq_queue_discovery"
                }));
            }
        }
    }
    
    Json(serde_json::json!({ 
        "services": discovered_services,
        "discovery_method": "rabbitmq_queue_discovery",
        "total_discovered": discovered_services.len()
    }))
}

/// Dynamically discover services by querying RabbitMQ management API
async fn discover_services_from_rabbitmq() -> Vec<String> {
    use reqwest;
    
    let mut services = Vec::new();
    
    // Query RabbitMQ management API for queues
    let client = reqwest::Client::new();
    let rmq_management = std::env::var("RABBITMQ_MANAGEMENT_URL").unwrap_or_else(|_| "http://localhost:15672".to_string());
    
    match client
        .get(&format!("{}/api/queues", rmq_management))
        .basic_auth(
            std::env::var("RABBITMQ_API_USER").unwrap_or_else(|_| "guest".to_string()), 
            Some(std::env::var("RABBITMQ_API_PASSWORD").unwrap_or_else(|_| "guest".to_string()))
        )
        .send()
        .await
    {
        Ok(response) => {
            if let Ok(queues) = response.json::<Vec<serde_json::Value>>().await {
                for queue in queues {
                    if let Some(queue_name) = queue["name"].as_str() {
                        // Extract service names from queue names
                        // Format: rabbitmesh.{service-name} or rabbitmesh.{service-name}.responses
                        if queue_name.starts_with("rabbitmesh.") && !queue_name.ends_with(".responses") {
                            let service_name = queue_name
                                .strip_prefix("rabbitmesh.")
                                .unwrap_or(queue_name);
                            
                            // Avoid duplicates
                            if !services.contains(&service_name.to_string()) {
                                services.push(service_name.to_string());
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            error!("Failed to discover services from RabbitMQ: {}", e);
        }
    }
    
    services
}

/// List services in format expected by client generator
async fn list_services_for_client_generator(State(state): State<GatewayState>) -> Json<Value> {
    let mut endpoints = Vec::new();
    
    // Dynamically discover services from RabbitMQ queues
    let discovered_service_names = discover_services_from_rabbitmq().await;
    
    for service_name in discovered_service_names {
        // Get methods for each service
        let methods = discover_service_methods(&state, &service_name).await;
        
        // Convert each method to the endpoint format expected by client generator
        for method in methods {
            if let Some(method_obj) = method.as_object() {
                // Get route and convert path parameters from :param to {param}
                let original_path = method_obj.get("path").and_then(|p| p.as_str()).unwrap_or("/unknown");
                let gateway_path = format!("/api/v1/{}{}", service_name, original_path
                    .replace(":id", "{id}")
                    .replace(":user_id", "{user_id}")
                    .replace(":order_id", "{order_id}")
                    .replace(":status", "{status}")
                    .replace(":email", "{email}")
                );
                
                let endpoint = serde_json::json!({
                    "service": service_name,
                    "handler": method_obj.get("name").unwrap_or(&serde_json::Value::String("unknown".to_string())),
                    "method": method_obj.get("http_method").unwrap_or(&serde_json::Value::String("GET".to_string())),
                    "path": gateway_path,
                    "description": method_obj.get("description").unwrap_or(&serde_json::Value::String("Auto-generated method".to_string())),
                    "parameters": [],
                    "response_type": "any"
                });
                endpoints.push(endpoint);
            }
        }
    }
    
    Json(serde_json::json!({
        "endpoints": endpoints,
        "discovery_method": "dynamic_rabbitmq_discovery",
        "total_endpoints": endpoints.len()
    }))
}

/// Dynamically discover service methods by querying the service
async fn discover_service_methods(state: &GatewayState, service_name: &str) -> Vec<serde_json::Value> {
    // Try to call a schema or introspection method on the service
    match state.service_client.call_with_timeout(
        service_name,
        "schema",
        serde_json::json!({}),
        Duration::from_secs(3)
    ).await {
        Ok(rabbitmesh::message::RpcResponse::Success { data, .. }) => {
            // Service returned its schema - data is the JSON object directly
            if let Some(methods) = data.get("methods").and_then(|m| m.as_array()) {
                return methods.iter()
                    .filter_map(|method| {
                        if let (Some(name), Some(route)) = (
                            method.get("name").and_then(|n| n.as_str()),
                            method.get("route").and_then(|r| r.as_str())
                        ) {
                            Some(serde_json::json!({
                                "name": name,
                                "route": route,
                                "http_method": method.get("http_method").and_then(|h| h.as_str()).unwrap_or("GET"),
                                "path": method.get("path").and_then(|p| p.as_str()).unwrap_or("/unknown"),
                                "description": method.get("description")
                                    .and_then(|d| d.as_str())
                                    .unwrap_or(&format!("Method: {}", name))
                            }))
                        } else {
                            None
                        }
                    })
                    .collect();
            }
        },
        Ok(rabbitmesh::message::RpcResponse::Error { error, .. }) => {
            tracing::warn!("Service {} schema call returned error: {}", service_name, error);
        },
        Err(e) => {
            tracing::debug!("Schema call failed for service {}: {}", service_name, e);
        }
    }

    // Fallback to basic ping method
    vec![
        serde_json::json!({
            "name": "ping",
            "route": "GET /ping",
            "http_method": "GET", 
            "path": "/ping",
            "description": "Health check endpoint"
        })
    ]
}


/// Describe a specific service and its methods
async fn describe_service(
    State(state): State<GatewayState>,
    Path(service): Path<String>,
) -> Result<Json<Value>, GatewayError> {
    // First check the service registry
    if let Some(service_info) = state.service_registry.get_service(&service).await {
        return Ok(Json(serde_json::json!(service_info)));
    }
    
    // If not in registry, check if it's a dynamically discovered service
    let discovered_services = discover_services_from_rabbitmq().await;
    if discovered_services.contains(&service) {
        // Try to ping the service to check its status
        let status = match state.service_client.call_with_timeout(
            &service,
            "ping",
            serde_json::json!({}),
            Duration::from_secs(2)
        ).await {
            Ok(_) => "healthy",
            Err(_) => "unreachable",
        };
        
        // Try to get methods from the service
        let methods = discover_service_methods(&state, &service).await;
        
        return Ok(Json(serde_json::json!({
            "name": service,
            "status": status,
            "discovery_method": "rabbitmq_queue_discovery",
            "description": format!("Dynamically discovered service: {}", service),
            "methods": methods,
            "total_methods": methods.len()
        })));
    }
    
    Err(GatewayError::ServiceNotFound(service))
}

/// Gateway-specific errors
#[derive(Debug)]
pub enum GatewayError {
    ServiceError {
        service: String,
        method: String,
        error: String,
        code: Option<String>,
        details: Option<Value>,
    },
    ServiceNotFound(String),
    RabbitMeshError(RabbitMeshError),
}

impl From<RabbitMeshError> for GatewayError {
    fn from(err: RabbitMeshError) -> Self {
        Self::RabbitMeshError(err)
    }
}

impl axum::response::IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            GatewayError::ServiceError { service, method, error, code, details } => {
                let status = match code.as_deref() {
                    Some("NOT_FOUND") => StatusCode::NOT_FOUND,
                    Some("UNAUTHORIZED") => StatusCode::UNAUTHORIZED,
                    Some("FORBIDDEN") => StatusCode::FORBIDDEN,
                    Some("BAD_REQUEST") => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                
                let body = serde_json::json!({
                    "error": error,
                    "service": service,
                    "method": method,
                    "code": code,
                    "details": details
                });
                
                (status, body)
            }
            GatewayError::ServiceNotFound(service) => {
                (
                    StatusCode::NOT_FOUND,
                    serde_json::json!({
                        "error": format!("Service '{}' not found", service),
                        "code": "SERVICE_NOT_FOUND"
                    })
                )
            }
            GatewayError::RabbitMeshError(err) => {
                error!("Gateway error: {}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    serde_json::json!({
                        "error": "Internal gateway error",
                        "code": "GATEWAY_ERROR"
                    })
                )
            }
        };

        (status, Json(error_message)).into_response()
    }
}