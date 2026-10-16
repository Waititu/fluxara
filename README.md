# Fluxara

**Reliable event flow. Engineered for scale.**

Fluxara is a production-oriented asynchronous event-processing platform built in Rust.

It is designed to evolve into a reliable system for accepting, persisting, routing, and processing events across distributed services.

## Current capabilities

- Asynchronous HTTP server using Axum and Tokio
- Backwards-compatible JSON health endpoint at `GET /health`
- Validated host and port configuration
- Generated request IDs returned in `x-request-id`
- Structured application logging and HTTP request tracing
- Consistent JSON errors for unknown routes
- Graceful shutdown on Ctrl-C and SIGTERM on Unix
- Unit and HTTP integration tests
- Automated formatting, linting, test, and release-build checks

## Requirements

- Rust stable toolchain
- Cargo
- Git

## Run locally

```bash
cargo run
