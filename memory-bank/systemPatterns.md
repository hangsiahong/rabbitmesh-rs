# System Patterns

## Architecture
**Event-Driven Gateway**: Users hit an edge HTTP API Gateway (`axum` based) which transforms their REST requests into RMQ Messages, forwards them to bounded queues (`rabbitmesh.ServiceName.MethodName`), then `.await`s an RMQ response queue.

**RPC Paradigm over AMQP**: Using AMQP `reply_to` and `correlation_id` fields, we emulate strict synchronous Request-Response flows natively on async durable AMQP TCP connections.

## Resilience and Fault Tolerance
**Poison Message Handling**: Implements a Dead Letter Exchange (DLX) `rabbitmesh.dlx` and DLQ `rabbitmesh.dlq`.
- **Transient Errors**: Requests that fail due to temporary networking/service issues are `Nack`'d with `requeue: true` for eventual consistency.
- **Fatal Errors**: Malformed payloads (poison messages) are `Nack`'d with `requeue: false`, dropping them safely into the DLQ without infinitely looping the worker.

## Framework Components
- `rabbitmesh`: Handles `ConnectionManager`, `RpcFramework`, the generic `Service` abstractions. 
- `rabbitmesh-macros`: Defines `#[service_definition]`, `#[service_impl]` and `#[service_method]`. Translates syntax trees to core RPC bindings.
- `rabbitmesh-gateway`: A proxy converting automatic Axum routers -> AMQP RPC via Dynamic Discovery.

## Coding Standards
- High usage of `tokio` async paradigms.
- Structured logging via `tracing`.
- Results passed as localized `Result<T, RabbitMeshError>`.
