use lapin::{
    options::*,
    types::FieldTable,
    Connection, Channel, Queue, Consumer,
    BasicProperties, publisher_confirm::Confirmation,
};
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};
use tracing::{info, warn, error, debug};
use crate::error::{RabbitMeshError, Result};
use std::time::Duration;

/// Configuration for AMQP connection
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// AMQP broker URL (e.g., "amqp://localhost:5672")
    pub url: String,
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
    /// Heartbeat interval in seconds
    pub heartbeat_seconds: u16,
    /// Number of connection retry attempts
    pub max_retries: u32,
    /// Delay between retry attempts in milliseconds
    pub retry_delay_ms: u64,
    /// Channel prefetch count for load balancing
    pub prefetch_count: u16,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            url: "amqp://localhost:5672".to_string(),
            connection_timeout_ms: 10_000,
            heartbeat_seconds: 60,
            max_retries: 5,
            retry_delay_ms: 1_000,
            prefetch_count: 10,
        }
    }
}

/// Manages AMQP connections with automatic reconnection and connection pooling
pub struct ConnectionManager {
    config: ConnectionConfig,
    connection: Arc<RwLock<Option<Arc<Connection>>>>,
    channels: Arc<Mutex<Vec<Channel>>>,
}

impl ConnectionManager {
    /// Create a new connection manager with default configuration
    pub fn new(url: impl Into<String>) -> Self {
        let mut config = ConnectionConfig::default();
        config.url = url.into();
        Self::with_config(config)
    }

    /// Create a new connection manager with custom configuration
    pub fn with_config(config: ConnectionConfig) -> Self {
        Self {
            config,
            connection: Arc::new(RwLock::new(None)),
            channels: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Establish connection to RabbitMQ with retry logic
    pub async fn connect(&self) -> Result<()> {
        let mut attempts = 0;
        let max_retries = self.config.max_retries;

        loop {
            match self.try_connect().await {
                Ok(connection) => {
                    info!("Successfully connected to RabbitMQ at {}", self.config.url);
                    *self.connection.write().await = Some(Arc::new(connection));
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_retries {
                        error!("Failed to connect to RabbitMQ after {} attempts: {}", attempts, e);
                        return Err(e);
                    }

                    warn!(
                        "Connection attempt {} failed, retrying in {}ms: {}",
                        attempts, self.config.retry_delay_ms, e
                    );
                    
                    tokio::time::sleep(Duration::from_millis(self.config.retry_delay_ms)).await;
                }
            }
        }
    }

    /// Internal method to attempt connection
    async fn try_connect(&self) -> Result<Connection> {
        debug!("Attempting to connect to {}", self.config.url);
        
        let connection = Connection::connect(
            &self.config.url,
            lapin::ConnectionProperties::default()
                .with_connection_name(format!("rabbitmesh-{}", uuid::Uuid::new_v4()).into())
        ).await?;

        debug!("AMQP connection established");
        Ok(connection)
    }

    /// Get or create a channel with automatic reconnection
    pub async fn get_channel(&self) -> Result<Channel> {
        // Try to reuse existing channel
        {
            let mut channels = self.channels.lock().await;
            if let Some(channel) = channels.pop() {
                if channel.status().connected() {
                    debug!("Reusing existing channel");
                    return Ok(channel);
                }
            }
        }

        // Create new channel
        let connection = self.ensure_connected().await?;
        let channel = connection.create_channel().await?;
        
        // Configure channel for load balancing
        channel.basic_qos(self.config.prefetch_count, BasicQosOptions::default()).await?;
        
        debug!("Created new channel");
        Ok(channel)
    }

    /// Return channel to pool for reuse
    pub async fn return_channel(&self, channel: Channel) {
        if channel.status().connected() {
            let mut channels = self.channels.lock().await;
            // Limit pool size to prevent memory leak
            if channels.len() < 10 {
                channels.push(channel);
                debug!("Returned channel to pool");
            }
        }
    }

    /// Ensure connection is established and healthy
    async fn ensure_connected(&self) -> Result<Arc<Connection>> {
        {
            let connection_guard = self.connection.read().await;
            if let Some(connection) = connection_guard.as_ref() {
                if connection.status().connected() {
                    return Ok(connection.clone());
                }
            }
        }
        
        // Need to reconnect
        warn!("Connection lost, attempting to reconnect");
        self.connect().await?;
        
        let connection_guard = self.connection.read().await;
        connection_guard
            .as_ref()
            .cloned()
            .ok_or_else(|| RabbitMeshError::internal_error("Connection should exist after connect"))
    }

    /// Declare a queue with optional arguments (e.g. for DLX)
    pub async fn declare_queue(&self, queue_name: &str, args: FieldTable) -> Result<Queue> {
        let channel = self.get_channel().await?;
        
        let queue = channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions {
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    ..Default::default()
                },
                args,
            )
            .await?;
        
        self.return_channel(channel).await;
        debug!("Declared queue: {}", queue_name);
        Ok(queue)
    }

    /// Declare a dead letter exchange (DLX)
    pub async fn declare_exchange(&self, name: &str, kind: lapin::ExchangeKind) -> Result<()> {
        let channel = self.get_channel().await?;
        
        channel.exchange_declare(
            name,
            kind,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        ).await?;
        
        self.return_channel(channel).await;
        info!("Declared Exchange: {}", name);
        Ok(())
    }

    /// Bind a queue to an exchange
    pub async fn bind_queue(&self, queue: &str, exchange: &str, routing_key: &str) -> Result<()> {
        let channel = self.get_channel().await?;
        
        channel.queue_bind(
            queue,
            exchange,
            routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        ).await?;
        
        self.return_channel(channel).await;
        info!("Bound queue {} to exchange {} with key {}", queue, exchange, routing_key);
        Ok(())
    }

    /// Create a consumer for the specified queue
    pub async fn create_consumer(&self, queue_name: &str, consumer_tag: &str) -> Result<Consumer> {
        let channel = self.get_channel().await?;
        
        let consumer = channel
            .basic_consume(
                queue_name,
                consumer_tag,
                BasicConsumeOptions {
                    no_ack: false,
                    exclusive: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        
        debug!("Created consumer for queue: {}", queue_name);
        Ok(consumer)
    }

    /// Publish a message to a queue
    pub async fn publish(
        &self,
        queue_name: &str,
        payload: &[u8],
        properties: BasicProperties,
    ) -> Result<Confirmation> {
        let channel = self.get_channel().await?;
        
        let confirmation = channel
            .basic_publish(
                "",
                queue_name,
                BasicPublishOptions::default(),
                payload,
                properties,
            )
            .await?
            .await?;
        
        self.return_channel(channel).await;
        debug!("Published message to queue: {}", queue_name);
        Ok(confirmation)
    }

    /// Check if connection is healthy
    pub async fn is_connected(&self) -> bool {
        let connection_guard = self.connection.read().await;
        connection_guard
            .as_ref()
            .map(|conn| conn.status().connected())
            .unwrap_or(false)
    }

    /// Get connection statistics for monitoring
    pub async fn get_stats(&self) -> ConnectionStats {
        let is_connected = self.is_connected().await;
        let channel_count = self.channels.lock().await.len();
        
        ConnectionStats {
            is_connected,
            channel_pool_size: channel_count,
            url: self.config.url.clone(),
        }
    }
}

/// Connection statistics for monitoring
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConnectionStats {
    pub is_connected: bool,
    pub channel_pool_size: usize,
    pub url: String,
}

impl std::fmt::Debug for ConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager")
            .field("config", &self.config)
            .finish()
    }
}