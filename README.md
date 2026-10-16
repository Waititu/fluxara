# Fluxara

**Reliable event flow. Engineered for scale.**

Fluxara is a production-oriented asynchronous event-processing
platform built in Rust.

It is designed to evolve into a reliable system for accepting,
persisting, routing, and processing events across distributed services.

## Current capabilities

- Asynchronous HTTP API using Axum and Tokio
- JSON health endpoint
- Structured application logging
- Automated Rust tests
- Formatting and linting checks

## Architecture roadmap

1. API foundations and engineering standards
2. Event ingestion and request validation
3. PostgreSQL persistence and database migrations
4. RabbitMQ publishing and asynchronous workers
5. Retry policies, idempotency, and dead-letter handling
6. Authentication, authorization, and rate limiting
7. Metrics, distributed tracing, and dashboards
8. Docker images and local integration testing
9. Kubernetes deployment and resource guardrails
10. Continuous integration, release automation, and benchmarks

## Requirements

- Rust stable toolchain
- Cargo
- Git

## Run locally

```bash
cargo run
