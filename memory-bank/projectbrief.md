# Project Brief

## Project Name
RabbitMesh - Zero-Port Microservices Framework

## Goal
Build a Rust-based async microservices framework that eliminates traditional HTTP port-management and complex service discovery by leveraging RabbitMQ (AMQP) for all inter-service communication. It allows developers to focus purely on business logic.

## Core Features
1. **Zero Port Management**: Services only connect to the RabbitMQ broker; they do not open individual HTTP ports.
2. **Auto-Generated API Gateway**: Converting AMQP rpc responses and requests into REST and GraphQL seamlessly using macros mapping.
3. **Macro-Driven Ergonomics**: A developer uses `#[service_definition]` and `#[service_method("POST /...")]` macros, and the framework auto-configures queues, routings, and serialization.
4. **Resilience**: The underlying connection is asynchronous and persistent, providing high throughput, automatic load balancing via queues, and fault tolerance.

## Leftover Work Identified
1. Deepen the auto-generation logic from `#[service_method]` instead of generic placeholders in macros (`service_definition.rs`).
2. Integrate real database patterns and global states to replace mock architectures in the example services (like e-commerce `auth-service` and `user-service`).
3. Enhance the central Service Registry or Global State logic for dynamic handler lookups.
