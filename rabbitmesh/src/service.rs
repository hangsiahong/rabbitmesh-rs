use std::sync::Arc;
use tokio::sync::RwLock;
use lapin::message::Delivery;
use tracing::{info, error, debug, warn};
use uuid::Uuid;
use futures_util::StreamExt;

use crate::connection::{ConnectionManager, ConnectionConfig};
use crate::rpc::{RpcFramework, RpcHandler};
use crate::message::Message;
use crate::error::Result;

/// Configuration for microservice
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Service name (used for queue naming)
    pub service_name: String,
    /// AMQP connection configuration
    pub connection: ConnectionConfig,
    /// Maximum concurrent message processing
    pub max_concurrent_messages: usize,
    /// Health check interval in seconds
    pub health_check_interval_seconds: u64,
}

impl ServiceConfig {
    /// Create new service configuration
    pub fn new(service_name: impl Into<String>, amqp_url: impl Into<String>) -> Self {
        let mut connection_config = ConnectionConfig::default();
        connection_config.url = amqp_url.into();
        
        Self {
            service_name: service_name.into(),
            connection: connection_config,
            max_concurrent_messages: 100,
            health_check_interval_seconds: 30,
        }
    }
}

/// Core microservice that processes messages via RabbitMQ
/// 
/// This is the heart of the framework - it:
/// - Connects to RabbitMQ (no ports exposed)
/// - Processes messages concurrently with tokio-stream
/// - Handles RPC requests and responses
/// - Provides health checking and monitoring
#[derive(Debug)]
pub struct MicroService {
    /// Service configuration
    config: ServiceConfig,
    /// AMQP connection manager
    connection: Arc<ConnectionManager>,
    /// RPC framework for handling requests
    rpc: Arc<RpcFramework>,
    /// Service status
    status: Arc<RwLock<ServiceStatus>>,
}

/// Service operational status
#[derive(Debug, Clone)]
pub enum ServiceStatus {
    /// Service is starting up
    Starting,
    /// Service is running and processing messages
    Running,
    /// Service is shutting down
    ShuttingDown,
    /// Service has stopped
    Stopped,
    /// Service encountered an error
    Error(String),
}

impl MicroService {
    /// Create a new microservice
    pub async fn new(config: ServiceConfig) -> Result<Self> {
        info!("🚀 Creating microservice: {}", config.service_name);
        
        let connection = Arc::new(ConnectionManager::with_config(config.connection.clone()));
        let rpc = Arc::new(RpcFramework::new(connection.clone(), config.service_name.clone()));
        
        Ok(Self {
            config,
            connection,
            rpc,
            status: Arc::new(RwLock::new(ServiceStatus::Starting)),
        })
    }

    /// Convenience constructor with just service name and AMQP URL
    pub async fn new_simple(service_name: impl Into<String>, amqp_url: impl Into<String>) -> Result<Self> {
        let config = ServiceConfig::new(service_name, amqp_url);
        Self::new(config).await
    }

    /// Register an RPC handler for a specific method
    /// 
    /// This is how you add business logic to your service
    pub async fn register_handler<H>(&self, method: impl Into<String>, handler: H)
    where
        H: RpcHandler + 'static,
    {
        self.rpc.register_handler(method, handler).await;
    }

    /// Register a function-based handler (more convenient)
    pub async fn register_function<F, Fut>(&self, method: impl Into<String>, handler: F)
    where
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<crate::message::RpcResponse>> + Send + 'static,
    {
        self.rpc.register_function(method, handler).await;
    }

    /// Start the microservice (this is non-blocking and processes messages concurrently)
    pub async fn start(&self) -> Result<()> {
        info!("🏁 Starting microservice: {}", self.config.service_name);
        
        // Update status
        *self.status.write().await = ServiceStatus::Running;
        
        // Connect to RabbitMQ
        self.connection.connect().await?;
        info!("✅ Connected to RabbitMQ");

        // Setup Dead Letter Exchange (DLX) infrastructure for resilience
        let dlx_name = "rabbitmesh.dlx";
        let dlq_name = "rabbitmesh.dlq";
        
        // Declare Fanout DLX
        self.connection.declare_exchange(dlx_name, lapin::ExchangeKind::Fanout).await?;
        
        // Declare DLQ
        self.connection.declare_queue(dlq_name, lapin::types::FieldTable::default()).await?;
        
        // Bind DLQ to DLX
        self.connection.bind_queue(dlq_name, dlx_name, "").await?;
        info!("✅ Configured Resilience: DLX ({}) -> DLQ ({})", dlx_name, dlq_name);

        // Set up service queues with DLX arguments
        let request_queue = format!("rabbitmesh.{}", self.config.service_name);
        let response_queue = format!("rabbitmesh.{}.responses", self.config.service_name);
        
        let mut queue_args = lapin::types::FieldTable::default();
        queue_args.insert("x-dead-letter-exchange".into(), lapin::types::AMQPValue::LongString(dlx_name.into()));
        
        self.connection.declare_queue(&request_queue, queue_args.clone()).await?;
        self.connection.declare_queue(&response_queue, queue_args).await?;
        info!("✅ Declared service queues: {}, {}", request_queue, response_queue);

        // Start message processors concurrently
        let request_processor = self.start_request_processor().await?;
        let response_processor = self.start_response_processor().await?;
        let cleanup_task = self.start_cleanup_task().await?;
        let health_check_task = self.start_health_check_task().await?;

        info!("🎯 Microservice {} is running and ready to process messages", self.config.service_name);
        info!("📊 Max concurrent messages: {}", self.config.max_concurrent_messages);

        // Wait for all tasks to complete (they run indefinitely)
        tokio::try_join!(
            request_processor,
            response_processor, 
            cleanup_task,
            health_check_task,
        )?;

        Ok(())
    }

    /// Start processing incoming RPC requests
    async fn start_request_processor(&self) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let queue_name = format!("rabbitmesh.{}", self.config.service_name);
        let consumer_tag = format!("{}-requests-{}", self.config.service_name, Uuid::new_v4());
        
        let consumer = self.connection.create_consumer(&queue_name, &consumer_tag).await?;
        let rpc = self.rpc.clone();
        let service_name = self.config.service_name.clone();
        let max_concurrent = self.config.max_concurrent_messages;

        let handle = tokio::spawn(async move {
            info!("📥 Request processor started for {}", service_name);
            
            // Process messages from consumer stream
            let mut stream = consumer;
            while let Some(delivery_result) = stream.next().await {
                match delivery_result {
                    Ok(delivery) => {
                        let rpc_clone = rpc.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::process_request_message(delivery, rpc_clone).await {
                                // This log is for framework-level erors (e.g. Ack failure)
                                error!("Critical error in request processor: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Error receiving message: {}", e);
                    }
                }
            }

            warn!("Request processor stopped for {}", service_name);
            Ok(())
        });

        info!("✅ Request processor spawned for queue: {}", queue_name);
        Ok(handle)
    }

    /// Start processing incoming RPC responses
    async fn start_response_processor(&self) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let queue_name = format!("rabbitmesh.{}.responses", self.config.service_name);
        let consumer_tag = format!("{}-responses-{}", self.config.service_name, Uuid::new_v4());
        
        let consumer = self.connection.create_consumer(&queue_name, &consumer_tag).await?;
        let rpc = self.rpc.clone();
        let service_name = self.config.service_name.clone();

        let handle = tokio::spawn(async move {
            info!("📤 Response processor started for {}", service_name);
            
            // Process messages from consumer stream
            let mut stream = consumer;
            while let Some(delivery_result) = stream.next().await {
                match delivery_result {
                    Ok(delivery) => {
                        let rpc_clone = rpc.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::process_response_message(delivery, rpc_clone).await {
                                error!("Error processing response: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Error receiving response: {}", e);
                    }
                }
            }
            
            warn!("Response processor stopped for {}", service_name);
            Ok(())
        });

        info!("✅ Response processor spawned for queue: {}", queue_name);
        Ok(handle)
    }

    /// Process a single request message with Resilience patterns
    async fn process_request_message(
        delivery: Delivery,
        rpc: Arc<RpcFramework>,
    ) -> Result<()> {
        // 1. Try to deserialize the message
        let message = match Message::from_bytes(&delivery.data) {
            Ok(msg) => msg,
            Err(e) => {
                error!("❌ FATAL: Received invalid message format (Poison Message): {}", e);
                // Nack with requeue=false to send to DLX (Dead Letter Exchange)
                // This prevents the poison message from being reprocessed infinitely
                delivery.nack(lapin::options::BasicNackOptions {
                    multiple: false,
                    requeue: false, 
                }).await?;
                return Ok(());
            }
        };
        
        debug!("📨 Processing request: {} from {}", message.method, message.from);
        
        // 2. Handle the request
        let result = rpc.handle_request(message).await;
        
        // 3. Acknowledge or Retry logic
        match result {
            Ok(_) => {
                // Success (or handled business error), acknowledge
                delivery.ack(lapin::options::BasicAckOptions::default()).await?;
                debug!("✅ Request processed and acknowledged");
            }
            Err(e) => {
                // Framework/Network error occurred
                error!("❌ TRANSIENT: Request processing failed: {}", e);
                
                // For transient errors, we Requeue (requeue=true)
                // In a production system, we should have a backoff delay or retry count check here
                // For now, simple requeue allows eventual consistency
                
                // Optional: Add a small delay to prevent tight loop if broker is down
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                delivery.nack(lapin::options::BasicNackOptions {
                    multiple: false,
                    requeue: true, // Retry
                }).await?;
            }
        }

        Ok(())
    }

    /// Process a single response message
    async fn process_response_message(
        delivery: Delivery,
        rpc: Arc<RpcFramework>,
    ) -> Result<()> {
        let message = Message::from_bytes(&delivery.data)?;
        
        debug!("📨 Processing response for correlation_id: {:?}", message.correlation_id);
        
        let result = rpc.handle_response(message).await;
        
        // Always acknowledge responses
        delivery.ack(lapin::options::BasicAckOptions::default()).await?;
        
        if let Err(e) = result {
            warn!("Response processing warning: {}", e);
        }

        Ok(())
    }

    /// Start cleanup task for expired RPC calls
    async fn start_cleanup_task(&self) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let rpc = self.rpc.clone();
        let service_name = self.config.service_name.clone();

        let handle = tokio::spawn(async move {
            info!("🧹 Cleanup task started for {}", service_name);
            
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            
            loop {
                interval.tick().await;
                rpc.cleanup_expired_calls().await;
            }
        });

        Ok(handle)
    }

    /// Start health check task
    async fn start_health_check_task(&self) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let connection = self.connection.clone();
        let service_name = self.config.service_name.clone();
        let interval_seconds = self.config.health_check_interval_seconds;

        let handle = tokio::spawn(async move {
            info!("💓 Health check task started for {}", service_name);
            
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_seconds));
            
            loop {
                interval.tick().await;
                
                let stats = connection.get_stats().await;
                if !stats.is_connected {
                    warn!("⚠️  Service {} lost connection to RabbitMQ", service_name);
                    // Connection manager will handle reconnection automatically
                } else {
                    debug!("💓 Health check OK for {}", service_name);
                }
            }
        });

        Ok(handle)
    }

    /// Get RPC client for calling other services
    pub fn get_client(&self) -> ServiceClient {
        ServiceClient::new(self.rpc.clone())
    }

    /// Get service statistics
    pub async fn get_stats(&self) -> ServiceStats {
        let connection_stats = self.connection.get_stats().await;
        let rpc_stats = self.rpc.get_stats().await;
        let status = self.status.read().await.clone();

        ServiceStats {
            service_name: self.config.service_name.clone(),
            status,
            connection_stats,
            rpc_stats,
        }
    }

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        matches!(*self.status.read().await, ServiceStatus::Running) 
            && self.connection.is_connected().await
    }
}

/// Client for making RPC calls to other services
#[derive(Debug, Clone)]
pub struct ServiceClient {
    rpc: Arc<RpcFramework>,
}

impl ServiceClient {
    pub fn new(rpc: Arc<RpcFramework>) -> Self {
        Self { rpc }
    }

    /// Call another service method
    pub async fn call_service(
        &self,
        target_service: impl Into<String>,
        method: impl Into<String>,
        params: impl serde::Serialize,
    ) -> Result<crate::message::RpcResponse> {
        self.rpc.call_service(target_service, method, params).await
    }

    /// Call with custom timeout
    pub async fn call_service_with_timeout(
        &self,
        target_service: impl Into<String>,
        method: impl Into<String>,
        params: impl serde::Serialize,
        timeout: tokio::time::Duration,
    ) -> Result<crate::message::RpcResponse> {
        self.rpc.call_service_with_timeout(target_service, method, params, timeout).await
    }
}

/// Service statistics for monitoring
#[derive(Debug, Clone)]
pub struct ServiceStats {
    pub service_name: String,
    pub status: ServiceStatus,
    pub connection_stats: crate::connection::ConnectionStats,
    pub rpc_stats: crate::rpc::RpcStats,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::RpcResponse;

    #[tokio::test]
    async fn test_service_creation() {
        let service = MicroService::new_simple("test-service", "amqp://localhost:5672").await.unwrap();
        assert!(matches!(*service.status.read().await, ServiceStatus::Starting));
    }

    #[tokio::test] 
    async fn test_handler_registration() {
        let service = MicroService::new_simple("test-service", "amqp://localhost:5672").await.unwrap();
        
        service.register_function("test_method", |_msg| async {
            Ok(RpcResponse::success("test result", 10).unwrap())
        }).await;

        let stats = service.get_stats().await;
        assert_eq!(stats.rpc_stats.registered_handlers, 1);
    }
}