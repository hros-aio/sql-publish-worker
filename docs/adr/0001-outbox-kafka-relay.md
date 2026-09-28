# ADR 0001: HROS Outbox Kafka Relay Architecture

## Status
Accepted

## Context
HROS uses PostgreSQL as the operational database for domain services (`setting`, `access`, and `directory`) and Apache Kafka as the distributed event streaming backbone. To reliably publish domain events while maintaining database atomicity, services record events into an `outbox_events` table within their local transactional boundary. A background relay worker is needed to poll pending events and publish them to Kafka.

## Decision Drivers
1. **Single Reusable Binary**: Avoid maintaining three distinct worker codebases for setting, access, and directory.
2. **Database-Per-Service Isolation**: Strict isolation without cross-service database access or shared transaction contexts.
3. **At-Least-Once Delivery**: Downstream consumers deduplicate based on canonical `eventId` (originating from `outbox_events.id`).
4. **Zero-Migration Compliance**: Work strictly within the provided `outbox_events` table and `outbox_status` ENUM (`'PENDING', 'PUBLISHED', 'FAILED'`).
5. **Horizontal Scalability**: Support multiple worker replicas safely via PostgreSQL `FOR UPDATE SKIP LOCKED`.

## Architectural Decisions

1. **CLI & Domain Namespace Loading**:
   - Implemented via Cobra CLI (`hros-event-worker run --type=<setting|access|directory> --config=<path>`).
   - Configuration is loaded into domain-specific namespaces with ENV variables overriding YAML configurations.

2. **Claiming & Lock Management**:
   - Two-phase claim model: Short transactions claim batches using `FOR UPDATE SKIP LOCKED`.
   - Kafka publishing happens outside database transaction blocks to prevent long-held row locks.
   - Batch status update to `PUBLISHED` happens in a short commit upon receiving Kafka broker acknowledgement (`acks=all`).

3. **Event Envelope & Idempotency**:
   - `outbox_events.id` is mapped directly to `eventId` in the Kafka event envelope.
   - Partition keys are deterministically generated from `aggregate_id` to guarantee in-order delivery per aggregate.

4. **Retry & Error Isolation (Option A)**:
   - In-memory retry policy with exponential backoff and jitter handles transient network glitches.
   - Poisoned or unroutable records transition to `FAILED` in the database after retry exhaustion, allowing the batch to progress without head-of-line blocking.

5. **Observability & Operational Health**:
   - Structured JSON logging with `slog` omitting sensitive secrets and payloads.
   - Prometheus metrics on `/metrics` with low-cardinality labels (`worker_type`, `event_type`, `topic`).
   - Standard `/healthz` (liveness) and `/readyz` (readiness) HTTP probes.

## Consequences
- Guaranteed at-least-once delivery; consumers must remain idempotent based on `eventId`.
- Zero database locks held during network-bound Kafka latency spikes.
- Simplified operational model in Kubernetes (single container image deployed with domain-specific CLI flags).
