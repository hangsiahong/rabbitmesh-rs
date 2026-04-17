# Tech Context

## Core Technologies
- Language: **Rust 1.70+** (specifically leveraging 2024 idioms in `Cargo.toml`).
- AMQP Client: **`lapin` 2.5** for high concurrent RabbitMQ interactions.
- Async Runtime: **`tokio` 1.40**.
- HTTP Web Server (Gateway): **`axum` 0.8** & `tower`.
- Macros Parser: **`syn`**, **`quote`**, **`proc-macro2`**.
- Serialization: **`serde`**, **`serde_json`**.
- API Docs: **`utoipa`** for Swagger/OpenAPI outputs.
- Caching: **`redis` 0.24** connection manager for distributed caching.

## Deployment / Operations constraints
- Target environment needs an active RabbitMQ server standard cluster.
- Gateway scales independently of microservices instance count.
