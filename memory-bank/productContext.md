# Product Context

## Why this exists
Microservice communications using pure HTTP result in unmanageable port allocations, complex service discovery (needs external mesh like Istio or Consul), and blocking workloads. RabbitMesh simplifies all of this by routing all inter-service and gateway bound requests through a central RabbitMQ broker using AMQP.

## Value Proposition
- **Scale Seamlessly**: Deploy multiple instances of the same service naturally; RabbitMQ's built-in fanout/direct exchanges balance the load.
- **Developer Experience**: "Write business logic, get APIs". A developer never has to interact directly with Axios, Axum routing parameters, tracing, and serializing/deserializing HTTP verbs at the lowest level.
- **High Performance**: Native async Rust scaling natively without socket limitations per service port.

## Target Audience
Backend developers in Rust looking for a productive microservice framework focusing purely on domain logic.
