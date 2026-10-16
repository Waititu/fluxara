# Fluxara

**Reliable event flow. Engineered for scale.**

Fluxara is a production-oriented asynchronous event-processing platform built in Rust.

It is designed to evolve into a reliable system for accepting, persisting, routing, and processing events across distributed services.

## Current capabilities

- Asynchronous HTTP server using Axum and Tokio
- Backwards-compatible JSON health endpoint at `GET /health`
- Versioned event-ingestion API (`POST /v1/events`, `GET /v1/events/{id}`, `POST /v1/events/batch`)
- Strict event validation with consistent JSON errors
- Idempotent ingestion with conflict detection on event-ID reuse
- Concurrency-safe in-memory event store (local development and API tests)
- Validated host and port configuration
- Generated request IDs returned in `x-request-id`
- Structured application logging and HTTP request tracing
- Consistent JSON errors for unknown routes
- Graceful shutdown on Ctrl-C and SIGTERM on Unix
- Unit and HTTP integration tests
- Automated formatting, linting, test, and release-build checks

> **Durability warning:** accepted events are held in an in-memory store and
> **are lost on process restart**. This store is intentional for local
> development and API tests in Phase 3; it is **not production-durable**.
> Phase 4 will introduce PostgreSQL-backed persistence and migrations.

## Requirements

- Rust stable toolchain
- Cargo
- Git

## Architecture and module layout

```text
src/
  main.rs          Binary: logging setup, config, listener, graceful shutdown
  lib.rs           Library root
  config.rs        FLUXARA_HOST / FLUXARA_PORT parsing and validation
  application.rs   Health service snapshot
  error.rs         Shared JSON error envelope (AppError -> status + code + message)
  events/
    mod.rs         Re-exports and documented limits surface
    model.rs       EventSubmission (inbound), BatchRequest, AcceptedEvent (stored)
    validation.rs  Strict validation rules, normalization, size limits
    store.rs       EventStore trait + InMemoryEventStore (Phase 4 swaps this)
  api/
    mod.rs         Router: /health + /v1/* routes, request IDs, tracing
    events.rs      Event handlers (single create, fetch, batch)
tests/
  api.rs           Health contract + unknown-route JSON errors
  events.rs        Event API integration tests
```

Design notes:

- New dependencies are limited to `chrono` (RFC 3339 timestamps, with `serde`
  support) and `serde_json` (promoted from dev-dependencies: request parsing,
  payload handling, and response bodies all require it at runtime).
- Axum's built-in 2 MiB body cap is disabled (`DefaultBodyLimit::disable`)
  because it returns a plain-text 413. Event handlers enforce
  `MAX_REQUEST_BYTES` themselves so every oversized request gets the shared
  JSON error envelope.
- `EventStore` is the seam for Phase 4: a PostgreSQL implementation can
  replace `InMemoryEventStore` without changing the HTTP contract.
- Idempotency comparison is semantic on normalized content (`id`,
  `event_type`, `schema_version`, `occurred_at` normalized to UTC RFC 3339,
  `payload` by JSON value equality). Server-assigned `received_at` is
  excluded from the comparison.

## Event envelope (v1)

Single-event request body:

```json
{
  "id": "evt_01HXYZ",
  "event_type": "order.created",
  "schema_version": 1,
  "occurred_at": "2026-10-16T12:00:00Z",
  "payload": { "order_id": "123", "total": 99.99 }
}
```

Accepted-event response body (also returned by `GET /v1/events/{id}`):

```json
{
  "id": "evt_01HXYZ",
  "event_type": "order.created",
  "schema_version": 1,
  "occurred_at": "2026-10-16T12:00:00Z",
  "payload": { "order_id": "123", "total": 99.99 },
  "received_at": "2026-10-16T12:00:01Z"
}
```

Batch request body:

```json
{
  "events": [
    {
      "id": "evt_01HXYZ",
      "event_type": "order.created",
      "schema_version": 1,
      "occurred_at": "2026-10-16T12:00:00Z",
      "payload": { "order_id": "123" }
    }
  ]
}
```

Batch response body (`200 OK`, per-event outcomes):

```json
{
  "results": [
    {
      "id": "evt_01HXYZ",
      "status": 201,
      "result": "accepted",
      "event": { "id": "evt_01HXYZ", "event_type": "order.created", "schema_version": 1, "occurred_at": "2026-10-16T12:00:00Z", "payload": { "order_id": "123" }, "received_at": "2026-10-16T12:00:01Z" }
    },
    {
      "id": "evt_bad",
      "status": 422,
      "result": "invalid",
      "error": { "code": "unprocessable_entity", "message": "`event_type` must not be blank" }
    }
  ]
}
```

Error bodies always use the shared envelope:

```json
{ "error": { "code": "conflict", "message": "Event ID already exists with different content" } }
```

## Endpoints and status codes

| Method | Path               | Success                        | Errors                                                  |
| ------ | ------------------ | ------------------------------ | ------------------------------------------------------- |
| GET    | `/health`          | `200` service snapshot         | —                                                       |
| POST   | `/v1/events`       | `201` accepted, `200` replay   | `400`, `409`, `413`, `422`                              |
| GET    | `/v1/events/{id}`  | `200` accepted event           | `404` unknown ID (any format)                           |
| POST   | `/v1/events/batch` | `200` with per-event results   | `400`, `413`, `422` for a rejected batch envelope       |

Status semantics:

- `201 Created` — a single event was accepted for the first time.
- `200 OK` — an idempotent replay (single) or a batch envelope that was
  processed (inspect per-event `status`/`result` fields).
- `404 Not Found` (`code: "not_found"`) — unknown event ID or unknown route.
- `409 Conflict` (`code: "conflict"`) — an existing ID was reused with
  different content. The stored event is unchanged.
- `413 Payload Too Large` (`code: "payload_too_large"`) — request body,
  payload, or batch exceeded a configured limit.
- `422 Unprocessable Entity` (`code: "unprocessable_entity"`) — syntactically
  valid JSON that violates event validation rules.
- `400 Bad Request` (`code: "bad_request"`) — malformed JSON.

## Validation rules

- `id`: required string, 1–128 characters, ASCII letters/digits plus
  `.` `_` `-` `:`, and must start with a letter or digit.
- `event_type`: required string, non-blank after trimming, at most
  128 characters (stored trimmed).
- `schema_version`: required integer; only version `1` is accepted.
- `occurred_at`: required RFC 3339 timestamp string with timezone offset;
  validated and normalized to UTC (`2026-10-16T14:00:00+02:00` becomes
  `2026-10-16T12:00:00Z`).
- `payload`: required JSON value of any shape; its re-serialized size must
  not exceed 256 KiB (`MAX_PAYLOAD_BYTES = 262144`).
- Unknown fields are rejected; missing fields fail with `422`.
- Truncated or syntactically invalid JSON fails with `400`.

Configured limits:

| Constant                 | Value     | Meaning                                  |
| ------------------------ | --------- | ---------------------------------------- |
| `MAX_EVENT_ID_LEN`       | 128       | Max event ID characters                  |
| `MAX_EVENT_TYPE_LEN`     | 128       | Max event-type characters (trimmed)      |
| `SUPPORTED_SCHEMA_VERSION` | `1`     | Only accepted schema version             |
| `MAX_PAYLOAD_BYTES`      | 262144    | Max re-serialized payload (256 KiB)      |
| `MAX_BATCH_EVENTS`       | 100       | Max events per batch                     |
| `MAX_REQUEST_BYTES`      | 1048576   | Max request body for event routes (1 MiB)|

## Idempotency semantics

- The first valid submission of an event ID is accepted (`201`).
- Repeating the same ID with identical (normalized) content is an idempotent
  duplicate: the original stored event — including its original
  `received_at` — is returned with `200` and no second record is created.
- Reusing an ID with different content returns `409 Conflict`; the stored
  event is left untouched.
- Concurrent submissions of the same ID serialize on the store lock, so
  exactly one submission wins as accepted and the rest resolve to duplicate
  or conflict deterministically.
- No exactly-once processing is claimed: retries are safe (at-most-once
  storage per ID), but delivery and downstream processing guarantees are out
  of scope for this phase.

## Batch behavior and limits

- The batch envelope is `{ "events": [...] }` with 1–100 entries.
- Entries are processed **sequentially in request order**; duplicate IDs
  within a batch resolve against entries accepted earlier in the same batch
  (identical content → `duplicate`/`200`, different content →
  `conflict`/`409`).
- Batches are **not atomic**: entries accepted before a failure stay
  accepted. Callers must inspect per-event `status`/`result` values; a `200`
  batch response does not imply every entry was accepted.
- Per-event `result` values: `accepted` (201), `duplicate` (200),
  `conflict` (409), `invalid` (422), `too_large` (413).
- Envelope rejections: malformed JSON → `400`; missing/non-array/empty
  `events` → `422`; more than 100 entries or a body over 1 MiB → `413`.
- Limits are enforced before unbounded work: the body size is checked before
  parsing, the batch length before per-event validation.

## Run locally

```bash
cargo run
```

The server listens on `127.0.0.1:3000` by default. Configure with:

```bash
FLUXARA_HOST=127.0.0.1 FLUXARA_PORT=3000 cargo run
```

Curl examples (server on `127.0.0.1:3000`):

```bash
# Health
curl -i http://127.0.0.1:3000/health

# Create an event (201 on first accept)
curl -i -X POST http://127.0.0.1:3000/v1/events \
  -H 'content-type: application/json' \
  -d '{"id":"evt_01","event_type":"order.created","schema_version":1,"occurred_at":"2026-10-16T12:00:00Z","payload":{"order_id":"123"}}'

# Replay the identical event (200 duplicate)
curl -i -X POST http://127.0.0.1:3000/v1/events \
  -H 'content-type: application/json' \
  -d '{"id":"evt_01","event_type":"order.created","schema_version":1,"occurred_at":"2026-10-16T12:00:00Z","payload":{"order_id":"123"}}'

# Conflicting reuse of the ID (409)
curl -i -X POST http://127.0.0.1:3000/v1/events \
  -H 'content-type: application/json' \
  -d '{"id":"evt_01","event_type":"order.cancelled","schema_version":1,"occurred_at":"2026-10-16T12:00:00Z","payload":{"order_id":"123"}}'

# Retrieve an event (200) or miss (404)
curl -i http://127.0.0.1:3000/v1/events/evt_01
curl -i http://127.0.0.1:3000/v1/events/no-such-id

# Batch ingestion (200 with per-event results)
curl -i -X POST http://127.0.0.1:3000/v1/events/batch \
  -H 'content-type: application/json' \
  -d '{"events":[
    {"id":"evt_b1","event_type":"order.created","schema_version":1,"occurred_at":"2026-10-16T12:00:00Z","payload":{}},
    {"id":"evt_b1","event_type":"order.created","schema_version":1,"occurred_at":"2026-10-16T12:00:00Z","payload":{}}
  ]}'

# Validation failure (422)
curl -i -X POST http://127.0.0.1:3000/v1/events \
  -H 'content-type: application/json' \
  -d '{"id":"evt_bad","event_type":"   ","schema_version":1,"occurred_at":"2026-10-16T12:00:00Z","payload":{}}'
```

## Test and quality-gate commands

```bash
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
git diff --check
```

## Known limitations

- In-memory storage: accepted events are lost on restart; there is no
  persistence, replication, or migration story yet.
- No authentication, authorization, rate limiting, or multi-tenancy.
- No event routing, retries, or background processing (ingestion only).
- Bodies are buffered in memory up to `MAX_REQUEST_BYTES`; production
  deployments should add streaming limits at the edge.
- `received_at` uses the server clock; clock skew is not handled.

## Phase 4: PostgreSQL persistence and migrations

- Implement `EventStore` for PostgreSQL (unique constraint on event ID,
  transactional insert-or-compare for idempotency under concurrency).
- Add schema migrations (events table: id, event type, schema version,
  occurred timestamps, payload, received timestamp).
- Preserve the v1 HTTP contract and idempotency/conflict semantics.
- Add durability tests (restart recovery) and connection configuration.
