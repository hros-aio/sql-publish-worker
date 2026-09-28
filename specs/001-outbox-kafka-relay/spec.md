# Feature Specification: Outbox Kafka Relay Worker

**Feature Branch**: `001-outbox-kafka-relay`

**Created**: 2026-09-28

**Status**: Draft

**Input**: User description: "Implement HROS Outbox Kafka Relay Worker in Go as a standalone, generic infrastructure worker supporting setting, access, and directory domains"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Generic Outbox Event Relay for Domain Services (Priority: P1)

As a system platform operator, I want a single outbox relay worker that continuously scans domain outbox events and reliably publishes them to the message broker so downstream consumers receive timely and ordered domain updates without service-specific worker binaries.

**Why this priority**: Core value proposition. Relaying transactional outbox events to the message broker enables asynchronous communication and event-driven data flow across all HRMS domains.

**Independent Test**: Can be tested independently by launching the worker for a single domain (e.g., `directory`), inserting pending outbox records into that domain's database, and verifying that the records are delivered to the target broker topic in the expected envelope format and marked as published in the database.

**Acceptance Scenarios**:

1. **Given** pending outbox records exist in the domain database, **When** the worker executes a polling cycle, **Then** it claims a bounded batch, transforms each record into a standard event envelope, publishes them to the appropriate message broker topic using a stable partition key, and marks successfully published records as published with an accurate timestamp.
2. **Given** no pending outbox records exist, **When** the worker polls the database, **Then** it waits for the configured polling interval before checking again without busy-looping or consuming excessive resources.
3. **Given** the worker is running with type `setting`, `access`, or `directory`, **When** started via CLI, **Then** it automatically loads the corresponding domain configuration (database connection, topic prefix, broker targets) and operates strictly on that domain's boundary.

---

### User Story 2 - Failure Isolation and Retry Handling (Priority: P2)

As a site reliability engineer, I want the worker to handle transient broker and database failures gracefully and isolate failing events so a single poisoned or unroutable event does not halt batch processing or block other events.

**Why this priority**: High operational availability. In multi-tenant distributed environments, network blips or bad payloads must not freeze the event pipeline.

**Independent Test**: Can be tested independently by introducing simulated broker network failures or invalid payload records within a batch of valid records, and verifying that valid records continue to publish while failing records are handled according to retry policy.

**Acceptance Scenarios**:

1. **Given** a batch of records where one record encounters a transient publication failure, **When** the batch is processed, **Then** successful records are marked as published, and the failed record remains pending for retry according to the backoff policy without crashing the worker.
2. **Given** a record that repeatedly fails beyond the maximum retry threshold due to permanent errors, **When** the retry limit is exhausted, **Then** the record is marked as failed, an error log with structured context is emitted, and the failure counter metric is incremented.
3. **Given** the database or broker cluster is temporarily unreachable, **When** the worker attempts to poll or publish, **Then** it logs structured warnings, pauses with bounded exponential backoff with jitter, and reconnects without entering a rapid crash loop.

---

### User Story 3 - Safe Multi-Replica Scaling & At-Least-Once Delivery (Priority: P3)

As a platform operator, I want to run multiple replicas of the worker per domain so throughput scales horizontally without duplicate or conflicting concurrent record processing.

**Why this priority**: Scalability and high-throughput requirements across large enterprise tenants.

**Independent Test**: Can be tested independently by spinning up multiple worker instances targeting the same database and verifying that records are claimed exclusively across instances via row-level locking.

**Acceptance Scenarios**:

1. **Given** multiple worker instances connected to the same domain database, **When** they poll concurrently, **Then** each instance claims distinct sets of pending records without blocking each other or double-publishing records simultaneously.
2. **Given** a worker instance abruptly crashes after publishing a message to the broker but before committing the status update in the database, **When** another worker instance claims the pending record upon restart, **Then** it republishes the event using the exact same event identifier so downstream consumers can deduplicate safely.

---

### User Story 4 - Graceful Lifecycle Management & Operational Health (Priority: P4)

As a platform operations engineer, I want clean process signal handling and health checks so deployments and rolling restarts do not drop in-flight messages or cause downtime.

**Why this priority**: Reliability during continuous deployments (e.g., Kubernetes rolling updates).

**Independent Test**: Can be tested independently by issuing termination signals (SIGTERM/SIGINT) while events are being processed and verifying that in-flight batches finish and flush before process termination.

**Acceptance Scenarios**:

1. **Given** a running worker actively processing events, **When** a termination signal (`SIGTERM` or `SIGINT`) is received, **Then** the worker stops accepting new polling cycles, allows currently processing records to complete and acknowledge, flushes the broker producer, closes database connections, and exits within the configured timeout.
2. **Given** external orchestrator health probing, **When** querying worker health/readiness, **Then** the worker accurately reflects database and message broker connectivity status.

---

### Edge Cases

- **Broker Disconnection During Batch**: If the message broker disconnects midway through a batch, records published before disconnection must be marked published; records unacknowledged must remain pending for retry on reconnection.
- **Empty Batch / Low Activity**: The worker must sleep for the configured poll interval when no rows are returned to avoid CPU or database IO spikes.
- **Out-of-Order Aggregate Events**: Partition key resolution must consistently assign the same aggregate identifier (or tenant-qualified aggregate ID) to the same partition to maintain strict per-aggregate ordering.
- **Malformed Event Payload**: If an outbox event contains malformed JSON or an invalid structure that cannot be serialized into the envelope, the worker must isolate the event, record an error, mark it failed after policy exhaustion, and continue processing remaining events in the batch.
- **Invalid CLI Arguments or Configuration**: If an unsupported domain type is supplied (e.g., `--type=billing`) or required database/broker parameters are missing, the worker must fail fast with a non-zero exit code and explicit error message before initiating network connections.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST provide a single unified CLI binary supporting exactly three domain worker types: `setting`, `access`, and `directory`.
- **FR-002**: The CLI MUST accept `--type` and `--config` flags under a `run` command and reject unsupported worker types with a descriptive error and non-zero exit code.
- **FR-003**: The configuration subsystem MUST support hierarchical resolution: environment variables override configuration files, which override built-in defaults.
- **FR-004**: Database credentials and secrets MUST NEVER be logged or exposed in plain text.
- **FR-005**: The worker MUST strictly respect the database-per-service isolation boundary, connecting only to the database configured for the specified domain type.
- **FR-006**: The worker MUST atomically claim pending outbox records using row-level locking (`FOR UPDATE SKIP LOCKED`) in bounded batch sizes.
- **FR-007**: The worker MUST map outbox records to a standard event envelope, preserving the original `outbox_events.id` as the canonical `eventId` and preserving `tenant_code` without modification.
- **FR-008**: The worker MUST resolve destination broker topics dynamically based on `event_type` and domain prefix rules without hardcoding topic names in domain logic.
- **FR-009**: The worker MUST use a stable partition key derived from the aggregate identifier (`aggregate_id` or `tenant_code:aggregate_id`) to ensure in-order delivery per aggregate.
- **FR-010**: The worker MUST require message broker acknowledgement before marking any outbox record as `PUBLISHED` with the publication timestamp.
- **FR-011**: The worker MUST provide at-least-once delivery semantics and avoid long-running database transactions while waiting for broker acknowledgements.
- **FR-012**: The worker MUST classify publication failures into transient (retryable with backoff) and permanent (marked as `FAILED` after max retry attempts).
- **FR-013**: The worker MUST emit structured logs containing domain type, event identifier, tenant code, aggregate details, and destination topic on publish success and error events.
- **FR-014**: The worker MUST expose operational Prometheus metrics (polls, claims, publish successes, failures, durations, queue backlog) using low-cardinality labels.
- **FR-015**: The worker MUST implement graceful shutdown on `SIGTERM`/`SIGINT`, draining in-flight events and closing producer and database connections within a configurable timeout.
- **FR-016**: The worker MUST provide lightweight health and readiness checks verifying database and broker connectivity.

### Key Entities

- **Outbox Event**: Represents an event recorded during a business transaction awaiting asynchronous publication. Key attributes include event identifier, tenant code, aggregate type, aggregate identifier, event type, event version, payload data, status (`PENDING`, `PUBLISHED`, `FAILED`), creation timestamp, and publication timestamp.
- **Standard Event Envelope**: The structured message payload delivered to the message bus. Key attributes include eventId, eventType, eventVersion, tenantCode, occurredAt, producer, correlationId, causationId, traceId, and the business payload.
- **Domain Worker Configuration**: Configuration parameters bound to a specific domain (`setting`, `access`, `directory`) defining database connection parameters, connection pooling limits, broker endpoints, topic prefixes, batch size, polling intervals, retry policies, concurrency, and shutdown timeouts.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of valid pending outbox records are published to the message broker within 2 seconds under normal polling configuration.
- **SC-002**: Zero cross-tenant data leakage or tenant code modifications during envelope mapping across all domain types.
- **SC-003**: System sustains horizontal replica scaling (e.g., 3+ replicas) without lock contention deadlocks or duplicate concurrent message publishing.
- **SC-004**: 100% of in-flight claimed events complete publication or safe drain during graceful shutdown within the configured shutdown timeout.
- **SC-005**: 100% of event failures are isolated, ensuring unaffected events in the same batch are published without disruption.
- **SC-006**: CLI validation rejects 100% of invalid worker types or missing mandatory configs before opening database or broker connections.

---

## Assumptions

- **Existing Outbox Schema**: The `outbox_events` table schema adheres to the provided specification (`id`, `tenant_code`, `aggregate_type`, `aggregate_id`, `event_type`, `event_version`, `payload`, `status`, `created_at`, `updated_at`, `published_at`). In-memory retry tracking (Option A) is employed for transient failures without requiring additional table columns.
- **Consumer Idempotency**: Downstream message consumers use the canonical `eventId` (originating from `outbox_events.id`) for deduplication to handle at-least-once delivery windows during unexpected worker crashes.
- **Tracing Context**: When distributed tracing identifiers (`traceId`, `correlationId`, `causationId`) are present in metadata/payload, they are forwarded in the envelope; otherwise, null/omitted fields are standard.
- **Target Deployment**: The worker runs as a containerized process in Kubernetes or standalone VMs, with one deployment instance per domain type sharing the same binary image.
