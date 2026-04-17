use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{RwLock, oneshot, Mutex};
use tokio::time::{timeout, Duration, Instant};
use uuid::Uuid;
use tracing::{debug, warn, error, info};

use crate::error::{RabbitMeshError, Result};
use crate::message::{Message, RpcResponse};
use crate::connection::ConnectionManager;

/// Type alias for async RPC handler functions
pub type RpcHandlerFn = Arc<dyn Fn(Message) -> Pin<Box<dyn Future<Output = Result<RpcResponse>> + Send>> + Send + Sync>;

/// Trait for implementing RPC handlers
#[async_trait]
pub trait RpcHandler: Send + Sync {
    /// Handle an RPC request message
    async fn handle(&self, message: Message) -> Result<RpcResponse>;
    
    /// Get handler name for debugging
    fn name(&self) -> &'static str {
        "RpcHandler"
    }
}

/// Simple function-based RPC handler
pub struct FunctionHandler<F> {
    name: &'static str,
    handler: F,
}

impl<F> FunctionHandler<F> {
    pub fn new(name: &'static str, handler: F) -> Self {
        Self { name, handler }
    }
}

#[async_trait]
impl<F, Fut> RpcHandler for FunctionHandler<F>
where
    F: Fn(Message) -> Fut + Send + Sync,
    Fut: Future<Output = Result<RpcResponse>> + Send,
{
    async fn handle(&self, message: Message) -> Result<RpcResponse> {
        (self.handler)(message).await
    }
    
    fn name(&self) -> &'static str {
        self.name
    }
}

/// Manages pending RPC calls with timeout and correlation
#[derive(Debug)]
pub struct PendingCall {
    /// Response sender
    pub response_tx: oneshot::Sender<Result<RpcResponse>>,
    /// Request timestamp for timeout calculation
    pub started_at: Instant,
    /// Timeout duration
    pub timeout: Duration,
}

/// Core RPC framework for managing requests and responses
pub struct RpcFramework {
    /// Connection manager for AMQP
    connection: Arc<ConnectionManager>,
    /// Service name for this instance
    service_name: String,
    /// Registered RPC handlers
    handlers: Arc<RwLock<HashMap<String, Arc<dyn RpcHandler>>>>,
    /// Pending outgoing RPC calls
    pending_calls: Arc<Mutex<HashMap<Uuid, PendingCall>>>,
    /// Default timeout for RPC calls
    default_timeout: Duration,
}

impl RpcFramework {
    /// Create new RPC framework
    pub fn new(connection: Arc<ConnectionManager>, service_name: impl Into<String>) -> Self {
        Self {
            connection,
            service_name: service_name.into(),
            handlers: Arc::new(RwLock::new(HashMap::new())),
            pending_calls: Arc::new(Mutex::new(HashMap::new())),
            default_timeout: Duration::from_secs(30),
        }
    }

    /// Register an RPC handler for a specific method
    pub async fn register_handler<H>(&self, method: impl Into<String>, handler: H)
    where
        H: RpcHandler + 'static,
    {
        let method = method.into();
        let mut handlers = self.handlers.write().await;
        handlers.insert(method.clone(), Arc::new(handler));
        info!("Registered RPC handler for method: {}", method);
    }

    /// Register a function-based handler
    pub async fn register_function<F, Fut>(&self, method: impl Into<String>, handler: F)
    where
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RpcResponse>> + Send + 'static,
    {
        let method = method.into();
        let function_handler = FunctionHandler::new(
            Box::leak(method.clone().into_boxed_str()),
            handler,
        );
        self.register_handler(method, function_handler).await;
    }

    /// Make an RPC call to another service
    pub async fn call_service(
        &self,
        target_service: impl Into<String>,
        method: impl Into<String>,
        params: impl serde::Serialize,
    ) -> Result<RpcResponse> {
        self.call_service_with_timeout(target_service, method, params, self.default_timeout)
            .await
    }

    /// Make an RPC call with custom timeout
    pub async fn call_service_with_timeout(
        &self,
        target_service: impl Into<String>,
        method: impl Into<String>,
        params: impl serde::Serialize,
        timeout_duration: Duration,
    ) -> Result<RpcResponse> {
        let target_service = target_service.into();
        let method = method.into();

        // Create request message
        let request = Message::new_request(&self.service_name, &target_service, &method, params)?;
        let correlation_id = request.correlation_id.unwrap();

        // Set up response channel
        let (response_tx, response_rx) = oneshot::channel();
        
        {
            let mut pending_calls = self.pending_calls.lock().await;
            pending_calls.insert(
                correlation_id,
                PendingCall {
                    response_tx,
                    started_at: Instant::now(),
                    timeout: timeout_duration,
                },
            );
        }

        // Send request
        let queue_name = format!("rabbitmesh.{}", target_service);
        let payload = request.to_bytes()?;
        
        self.connection
            .publish(
                &queue_name,
                &payload,
                lapin::BasicProperties::default()
                    .with_correlation_id(correlation_id.to_string().into())
                    .with_reply_to(format!("rabbitmesh.{}.responses", self.service_name).into()),
            )
            .await?;

        debug!(
            "Sent RPC request to {}: {} (correlation_id: {})",
            target_service, method, correlation_id
        );

        // Wait for response with timeout
        match timeout(timeout_duration, response_rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                // Channel closed without response
                self.pending_calls.lock().await.remove(&correlation_id);
                Err(RabbitMeshError::internal_error("Response channel closed"))
            }
            Err(_) => {
                // Timeout occurred
                self.pending_calls.lock().await.remove(&correlation_id);
                Err(RabbitMeshError::Timeout {
                    timeout_ms: timeout_duration.as_millis() as u64,
                })
            }
        }
    }

    /// Handle incoming request message
    pub async fn handle_request(&self, message: Message) -> Result<()> {
        let method = message.method.clone();
        let start_time = Instant::now();
        
        debug!(
            "Handling RPC request: {} from {}",
            method, message.from
        );

        // Find handler
        let handler = {
            let handlers = self.handlers.read().await;
            handlers.get(&method).cloned()
        };

        let response = match handler {
            Some(handler) => {
                // Apply strict execution timeout binding (30s max)
                match tokio::time::timeout(std::time::Duration::from_secs(30), handler.handle(message.clone())).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(e)) => {
                        error!("Handler error for {}: {}", method, e);
                        RpcResponse::error(format!("Handler error: {}", e))
                    }
                    Err(_) => {
                        error!("FATAL: Handler timed out exceeding 30s. Dropping processing. Method: {}", method);
                        RpcResponse::error(format!("Execution timeout (30s boundary exceeded)"))
                    }
                }
            }
            None => {
                warn!("No handler found for method: {}", method);
                RpcResponse::error_detailed(
                    "Method not found",
                    "METHOD_NOT_FOUND",
                    format!("No handler registered for method: {}", method),
                )?
            }
        };

        let processing_time = start_time.elapsed().as_millis() as u64;
        
        // Add processing time to successful responses
        let final_response = match response {
            RpcResponse::Success { data, .. } => RpcResponse::Success { data, processing_time_ms: processing_time },
            error_response => error_response,
        };

        // Send response back
        let response_message = final_response.into_message(&message, &self.service_name)?;
        self.send_response(response_message).await?;

        debug!(
            "Completed RPC request: {} ({}ms)",
            method, processing_time
        );

        Ok(())
    }

    /// Handle incoming response message
    pub async fn handle_response(&self, message: Message) -> Result<()> {
        let correlation_id = message.correlation_id.ok_or_else(|| {
            RabbitMeshError::InvalidMessage { 
                reason: "Response missing correlation ID".to_string() 
            }
        })?;

        debug!(
            "Received RPC response for correlation_id: {}",
            correlation_id
        );

        // Find pending call
        let pending_call = {
            let mut pending_calls = self.pending_calls.lock().await;
            pending_calls.remove(&correlation_id)
        };

        if let Some(pending_call) = pending_call {
            let response: RpcResponse = message.deserialize_payload()?;
            
            if let Err(_) = pending_call.response_tx.send(Ok(response)) {
                warn!("Failed to send response to caller (receiver dropped)");
            }
        } else {
            warn!(
                "Received response for unknown correlation_id: {}",
                correlation_id
            );
        }

        Ok(())
    }

    /// Send response message
    async fn send_response(&self, response: Message) -> Result<()> {
        let target_service = response.to.as_ref().ok_or_else(|| {
            RabbitMeshError::InvalidMessage { 
                reason: "Response missing target service".to_string() 
            }
        })?;

        let queue_name = format!("rabbitmesh.{}.responses", target_service);
        let payload = response.to_bytes()?;
        
        self.connection
            .publish(
                &queue_name,
                &payload,
                lapin::BasicProperties::default()
                    .with_correlation_id(
                        response.correlation_id
                            .map(|id| id.to_string())
                            .unwrap_or_default()
                            .into()
                    ),
            )
            .await?;

        Ok(())
    }

    /// Clean up expired pending calls
    pub async fn cleanup_expired_calls(&self) {
        let now = Instant::now();
        let mut pending_calls = self.pending_calls.lock().await;
        
        let expired_calls: Vec<Uuid> = pending_calls
            .iter()
            .filter_map(|(id, call)| {
                if now.duration_since(call.started_at) > call.timeout {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        for id in expired_calls {
            if let Some(call) = pending_calls.remove(&id) {
                let _ = call.response_tx.send(Err(RabbitMeshError::Timeout {
                    timeout_ms: call.timeout.as_millis() as u64,
                }));
                debug!("Cleaned up expired call: {}", id);
            }
        }
    }

    /// Get framework statistics
    pub async fn get_stats(&self) -> RpcStats {
        let handlers_count = self.handlers.read().await.len();
        let pending_calls_count = self.pending_calls.lock().await.len();
        
        RpcStats {
            service_name: self.service_name.clone(),
            registered_handlers: handlers_count,
            pending_calls: pending_calls_count,
            default_timeout_ms: self.default_timeout.as_millis() as u64,
        }
    }
}

/// RPC framework statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct RpcStats {
    pub service_name: String,
    pub registered_handlers: usize,
    pub pending_calls: usize,
    pub default_timeout_ms: u64,
}

impl std::fmt::Debug for RpcFramework {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcFramework")
            .field("service_name", &self.service_name)
            .field("default_timeout", &self.default_timeout)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionConfig;
    
    #[tokio::test]
    async fn test_handler_registration() {
        let config = ConnectionConfig::default();
        let connection = Arc::new(ConnectionManager::with_config(config));
        let rpc = RpcFramework::new(connection, "test-service");
        
        // Register a simple handler
        rpc.register_function("test_method", |_msg| async {
            Ok(RpcResponse::success("test result", 10)?)
        }).await;
        
        let stats = rpc.get_stats().await;
        assert_eq!(stats.registered_handlers, 1);
        assert_eq!(stats.pending_calls, 0);
    }
}