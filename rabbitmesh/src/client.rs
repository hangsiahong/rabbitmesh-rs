use std::sync::Arc;
use tokio::time::Duration;
use tracing::{debug, info};

use crate::connection::{ConnectionManager, ConnectionConfig};
use crate::rpc::RpcFramework;
use crate::message::RpcResponse;
use crate::error::Result;

/// Standalone client for calling microservices from external applications
/// 
/// This is perfect for:
/// - API Gateway calling microservices
/// - Testing microservices
/// - Integration between different systems
#[derive(Debug, Clone)]
pub struct ServiceClient {
    rpc: Arc<RpcFramework>,
    connection: Arc<ConnectionManager>,
    client_name: String,
}

impl ServiceClient {
    /// Create a new service client
    pub async fn new(client_name: impl Into<String>, amqp_url: impl Into<String>) -> Result<Self> {
        let client_name = client_name.into();
        info!("🔗 Creating service client: {}", client_name);

        let mut config = ConnectionConfig::default();
        config.url = amqp_url.into();

        let connection = Arc::new(ConnectionManager::with_config(config));
        let rpc = Arc::new(RpcFramework::new(connection.clone(), client_name.clone()));

        // Connect to RabbitMQ
        connection.connect().await?;
        info!("✅ Service client connected: {}", client_name);

        // Set up response queue for this client
        let response_queue = format!("rabbitmesh.{}.responses", client_name);
        connection.declare_queue(&response_queue, lapin::types::FieldTable::default()).await?;
        
        // Start response processor
        let client = Self {
            rpc: rpc.clone(),
            connection: connection.clone(),
            client_name: client_name.clone(),
        };
        
        client.start_response_processor().await?;
        info!("✅ Service client ready: {}", client_name);

        Ok(client)
    }

    /// Call a microservice method
    /// 
    /// This is how the API gateway calls microservices:
    /// ```rust,no_run
    /// # use rabbitmesh::ServiceClient;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// let client = ServiceClient::new("api-gateway", "amqp://localhost:5672").await?;
    /// let response = client.call("user-service", "get_user", 123).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call(
        &self,
        service: impl Into<String>,
        method: impl Into<String>,
        params: impl serde::Serialize,
    ) -> Result<RpcResponse> {
        let service = service.into();
        let method = method.into();
        
        debug!("🔄 Client {} calling {}.{}", self.client_name, service, method);
        
        self.rpc.call_service(service, method, params).await
    }

    /// Call with custom timeout
    pub async fn call_with_timeout(
        &self,
        service: impl Into<String>,
        method: impl Into<String>,
        params: impl serde::Serialize,
        timeout: Duration,
    ) -> Result<RpcResponse> {
        let service = service.into();
        let method = method.into();
        
        debug!(
            "🔄 Client {} calling {}.{} (timeout: {:?})",
            self.client_name, service, method, timeout
        );
        
        self.rpc.call_service_with_timeout(service, method, params, timeout).await
    }

    /// Start processing responses (internal)
    async fn start_response_processor(&self) -> Result<()> {
        let queue_name = format!("rabbitmesh.{}.responses", self.client_name);
        let consumer_tag = format!("{}-responses", self.client_name);
        
        let consumer = self.connection.create_consumer(&queue_name, &consumer_tag).await?;
        let rpc = self.rpc.clone();
        let client_name = self.client_name.clone();

        tokio::spawn(async move {
            use futures_util::StreamExt;
            debug!("📤 Response processor started for client: {}", client_name);
            
            // Process messages from consumer stream sequentially to avoid race conditions
            let mut stream = consumer;
            while let Some(delivery_result) = stream.next().await {
                match delivery_result {
                    Ok(delivery) => {
                        // Process response synchronously to avoid correlation ID race conditions
                        let message = crate::message::Message::from_bytes(&delivery.data);
                        match message {
                            Ok(message) => {
                                debug!("Received RPC response for correlation_id: {:?}", message.correlation_id);
                                if let Err(e) = rpc.handle_response(message).await {
                                    tracing::error!("Error processing response: {}", e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("Error deserializing response: {}", e);
                            }
                        }
                        
                        // Always acknowledge after processing
                        if let Err(e) = delivery.ack(lapin::options::BasicAckOptions::default()).await {
                            tracing::error!("Failed to acknowledge message: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error receiving response: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    /// Check if client is connected and healthy
    pub async fn is_healthy(&self) -> bool {
        self.connection.is_connected().await
    }

    /// Get client statistics
    pub async fn get_stats(&self) -> ClientStats {
        let connection_stats = self.connection.get_stats().await;
        let rpc_stats = self.rpc.get_stats().await;
        
        ClientStats {
            client_name: self.client_name.clone(),
            connection_stats,
            rpc_stats,
        }
    }
}

/// Client statistics for monitoring
#[derive(Debug, Clone)]
pub struct ClientStats {
    pub client_name: String,
    pub connection_stats: crate::connection::ConnectionStats,
    pub rpc_stats: crate::rpc::RpcStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_creation() {
        // This test requires RabbitMQ running
        if std::env::var("RABBITMQ_URL").is_ok() {
            let client = ServiceClient::new("test-client", "amqp://localhost:5672").await;
            assert!(client.is_ok());
        }
    }
}