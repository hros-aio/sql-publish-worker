# Research & Architecture Decisions: Outbox Kafka Relay Worker

**Feature**: Outbox Kafka Relay Worker  
**Date**: 2026-09-28  
**Spec**: [spec.md](./spec.md)

---

## 1. CLI Framework & Configuration Architecture

### Decision
- Use `github.com/spf13/cobra` for command-line interface structure (`hros-event-worker run --type=<setting|access|directory> --config=<path>`).
- Use YAML parsing (`gopkg.in/yaml.v3`) combined with typed environment variable mapping (`HROS_<TYPE>_*`) and default fallback resolution.

### Rationale
- Cobra provides enterprise standard subcommands, flag validation, help/version formatting, and clean POSIX-compliant exit handling.
- A dedicated strongly typed configuration loader ensures explicit schema-per-domain binding (`setting`, `access`, `directory`) and fail-fast validation before opening any database or broker connections.
- Environment variables override YAML configuration, matching twelve-factor app and Kubernetes ConfigMap/Secret injection best practices.

### Alternatives Considered
- `urfave/cli/v2`: Viable, but Cobra is standard across Kubernetes/Go cloud-native ecosystems and explicitly specified in requirements.
- Pure Viper automatic binding: Can obscure exact domain namespace struct decoding; strongly typed explicit Unmarshal with explicit ENV key mapping is more deterministic, robust, and testable.

---

## 2. PostgreSQL Access & Claiming Strategy (`FOR UPDATE SKIP LOCKED`)

### Decision
- Use `database/sql` with `github.com/jackc/pgx/v5/stdlib` (or `pgx/v5` pool) for connection pooling and PostgreSQL row operations.
- Implement a two-phase transactional claim model:
  1. Short transaction: Execute `SELECT ... FROM outbox_events WHERE status = 'PENDING' ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED` and immediately read claimed events into memory.
  2. Perform Kafka publication outside the database transaction.
  3. Short transaction: Batch update published IDs to `status = 'PUBLISHED', published_at = now()`, and permanently failed IDs to `status = 'FAILED'`.
  4. Transient failures remain `PENDING` for in-memory backoff or subsequent poll pickup.

### Rationale
- Holding a database transaction open while waiting for Kafka ACKs over the network introduces long-held row locks, table lock contention, and connection pool exhaustion during broker latency spikes.
- Bounded short transactions combined with `FOR UPDATE SKIP LOCKED` prevent concurrent replicas from claiming identical records while ensuring high throughput and resilience.
- Guarantees at-least-once delivery: if a worker crashes before step 3, uncommitted records remain `PENDING` and will be claimed and republished by surviving replicas upon restart.

### Alternatives Considered
- Holding DB transaction open across Kafka publish: Rejected due to lock contention risks under high load or broker timeouts.
- Adding intermediate `PROCESSING` state in DB: Rejected to maintain strict zero-migration compatibility with the frozen `outbox_status` ENUM (`'PENDING', 'PUBLISHED', 'FAILED'`).

---

## 3. Kafka Producer Implementation & Delivery Guarantees

### Decision
- Use `github.com/IBM/sarama` configured as a synchronous/buffered idempotent producer with `RequiredAcks = WaitForAll` (acks=all), `Retry.Max = 5`, and idempotent mode enabled.
- Derive canonical `eventId` directly from `outbox_events.id`.
- Partition key derived from `aggregate_id` (or `tenant_code:aggregate_id`) to ensure strict partition-level ordering per aggregate entity.
- Dynamic topic resolution: `event_type` (e.g. `directory.employee.created` -> `directory.employee`, with configurable prefix support).

### Rationale
- Sarama is the proven enterprise Kafka library in Go and standard in the HROS architecture.
- `acks=all` ensures events are committed to all in-sync replicas before marking them `PUBLISHED`.
- Preserving `outbox_events.id` as `eventId` guarantees idempotency on consumer sides.

### Alternatives Considered
- `confluent-kafka-go` (librdkafka CGO wrapper): Rejected to avoid CGO cross-compilation overhead and simplify lightweight scratch/alpine container builds.

---

## 4. Error Handling & In-Memory Retry Strategy (Option A)

### Decision
- Classify errors into:
  - **Transient Errors**: Network timeouts, broker leader election, temporary PostgreSQL connection drops. Handled with bounded exponential backoff with full jitter and retried within the worker loop.
  - **Permanent Errors**: Serialization errors, malformed payload JSON, invalid topic names.
- Since the current schema lacks `retry_count` and `next_retry_at` columns, in-memory retry tracking is maintained per batch attempt (Option A). If an event fails permanently or exceeds maximum transient retry attempts within the attempt lifecycle, it is updated to `FAILED` in the database with structured error logging and Prometheus metric increment.

### Rationale
- Complies with the schema freeze constraint while ensuring permanent bad data does not poison the batch or block subsequent valid outbox records.

---

## 5. Observability & Graceful Shutdown

### Decision
- **Structured Logging**: `log/slog` (standard library Go 1.21+) configured for JSON output with contextual attributes (`worker_type`, `event_id`, `tenant_code`, `aggregate_type`, `aggregate_id`, `event_type`, `topic`). Mask/omit secrets and raw sensitive payloads.
- **Metrics**: `github.com/prometheus/client_golang/prometheus` exposing standard counters and histograms (`outbox_events_claimed_total`, `outbox_events_published_total`, `outbox_events_failed_total`, `outbox_publish_duration_seconds`, etc.) on a dedicated `/metrics` endpoint.
- **Health Checks**: Standard `net/http` server exposing `/healthz` (liveness) and `/readyz` (readiness with DB and Kafka ping checks).
- **Graceful Shutdown**: Capture `SIGINT` and `SIGTERM`, cancel worker context, drain running batch publishing, flush Sarama producer, close DB connection pool, and exit cleanly within `worker.shutdownTimeout`.
