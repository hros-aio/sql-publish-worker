# Tasks: HROS Outbox Kafka Relay Worker

**Input**: Design documents from `/specs/001-outbox-kafka-relay/`  
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

## Format: `- [ ] [TaskID] [P?] [Story?] Description with file path`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization, module configuration, and foundational directory structure.

- [X] T001 Initialize Go module and configure dependencies in go.mod
- [X] T002 [P] Create directory structure per implementation plan (cmd/, internal/, deployments/, docs/)
- [X] T003 [P] Create configuration template in config.example.yaml

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core configuration, domain models, metrics, and logging infrastructure required across all user stories.

**⚠️ CRITICAL**: Must complete before implementing user stories.

- [X] T004 [P] Implement structured logger with JSON output and context fields in internal/observability/logging.go
- [X] T005 [P] Implement Prometheus metrics registry and definitions in internal/observability/metrics.go
- [X] T006 [P] Implement configuration structs and defaults in internal/config/config.go
- [X] T007 Implement YAML file and environment variable configuration loader in internal/config/loader.go
- [X] T008 Implement configuration validation logic in internal/config/validator.go
- [X] T009 [P] Implement Outbox entity and status definitions in internal/outbox/entity.go
- [X] T010 [P] Implement Standard Event Envelope model and mapper in internal/event/envelope.go and internal/event/mapper.go

**Checkpoint**: Foundation ready - user story implementation can begin.

---

## Phase 3: User Story 1 - Generic Outbox Event Relay (Priority: P1) 🎯 MVP

**Goal**: Relay pending outbox records from PostgreSQL to Kafka topics using a single reusable binary supporting `setting`, `access`, and `directory` domains.

**Independent Test**: Start worker with `--type=directory`, insert a pending outbox record, and verify successful Kafka publishing, topic resolution, and status update to `PUBLISHED`.

### Tests for User Story 1
- [X] T011 [P] [US1] Implement unit tests for configuration loader and validator in internal/config/loader_test.go
- [X] T012 [P] [US1] Implement unit tests for event mapper and envelope formatting in internal/event/mapper_test.go

### Implementation for User Story 1
- [X] T013 [P] [US1] Implement Kafka topic resolver in internal/kafka/topic_resolver.go and internal/kafka/topic_resolver_test.go
- [X] T014 [P] [US1] Define OutboxRepository interface in internal/outbox/repository.go
- [X] T015 [US1] Implement PostgreSQL Outbox repository with ClaimBatch and MarkPublished in internal/outbox/postgres_repository.go
- [X] T016 [P] [US1] Define EventPublisher interface in internal/kafka/publisher.go
- [X] T017 [US1] Implement Sarama synchronous Kafka publisher with idempotent mode and acks=all in internal/kafka/sarama_publisher.go
- [X] T018 [US1] Implement core Outbox batch processor loop in internal/outbox/processor.go
- [X] T019 [US1] Implement Cobra CLI root and run commands supporting --type and --config in cmd/root.go and cmd/run.go
- [X] T020 [US1] Implement application main entrypoint wiring Cobra commands in main.go

**Checkpoint**: User Story 1 functional - single binary can relay outbox events for all three service types.

---

## Phase 4: User Story 2 - Failure Isolation and Retry Handling (Priority: P2)

**Goal**: Handle transient network/broker errors with exponential backoff and isolate permanent record failures to prevent batch halting.

**Independent Test**: Introduce a corrupted event payload or simulated Kafka disconnect, and verify valid records publish while failed records are isolated and transitioned to `FAILED` after retry exhaustion.

### Tests for User Story 2
- [X] T021 [P] [US2] Implement unit tests for retry policy, backoff calculation, and error classification in internal/retry/policy_test.go
- [X] T022 [P] [US2] Implement unit tests for batch processor failure isolation in internal/outbox/processor_test.go

### Implementation for User Story 2
- [X] T023 [P] [US2] Implement retry policy, jittered backoff, and error classifier in internal/retry/policy.go
- [X] T024 [US2] Add MarkFailed method to OutboxRepository interface and implementation in internal/outbox/repository.go and internal/outbox/postgres_repository.go
- [X] T025 [US2] Integrate retry loop, transient backoff, and per-event failure isolation in internal/outbox/processor.go

**Checkpoint**: User Stories 1 & 2 functional - resilient batch processing with failure isolation.

---

## Phase 5: User Story 3 - Safe Multi-Replica Scaling & At-Least-Once Delivery (Priority: P3)

**Goal**: Support concurrent worker replicas without lock contention or duplicate processing using PostgreSQL `FOR UPDATE SKIP LOCKED` and stable partition keys.

**Independent Test**: Concurrently run multiple worker instances against the same outbox table and verify no duplicate claims or deadlocks occur.

### Tests for User Story 3
- [X] T026 [P] [US3] Implement concurrency unit tests for repository claim isolation in tests/integration/relay_integration_test.go

### Implementation for User Story 3
- [X] T027 [US3] Ensure PostgreSQL claiming executes bounded short transactions with FOR UPDATE SKIP LOCKED in internal/outbox/postgres_repository.go
- [X] T028 [US3] Enforce stable partition key derivation (aggregate_id / tenant_code:aggregate_id) in internal/kafka/sarama_publisher.go

**Checkpoint**: Multi-replica horizontal scaling verified with at-least-once delivery semantics.

---

## Phase 6: User Story 4 - Graceful Lifecycle Management & Operational Health (Priority: P4)

**Goal**: Provide clean signal termination (`SIGTERM`/`SIGINT`), in-flight batch draining, and HTTP endpoints for `/healthz`, `/readyz`, and `/metrics`.

**Independent Test**: Send `SIGTERM` during active processing and query `/healthz` and `/metrics` endpoints.

### Tests for User Story 4
- [X] T029 [P] [US4] Implement unit tests for health server and worker runner in internal/health/server_test.go and internal/worker/runner_test.go

### Implementation for User Story 4
- [X] T030 [P] [US4] Implement HTTP health check and metrics server in internal/health/server.go
- [X] T031 [P] [US4] Implement version CLI command in cmd/version.go
- [X] T032 [US4] Implement worker runner supervisor with signal handling, batch drain, and connection cleanup in internal/worker/runner.go

**Checkpoint**: Production-ready lifecycle management, liveness/readiness probes, and Prometheus metrics.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Deployment manifests, integration tests, Docker containerization, and documentation.

- [X] T033 [P] Create multi-stage Go Dockerfile in deployments/docker/Dockerfile
- [X] T034 [P] Create Kubernetes Deployment manifests for setting, access, and directory in deployments/k8s/setting-deployment.yaml, deployments/k8s/access-deployment.yaml, and deployments/k8s/directory-deployment.yaml
- [X] T035 [P] Implement end-to-end integration test suite in tests/integration/relay_integration_test.go
- [X] T036 [P] Write Architecture Decision Record in docs/adr/0001-outbox-kafka-relay.md and project documentation in README.md
- [X] T037 Validate end-to-end flow against specs/001-outbox-kafka-relay/quickstart.md

---

## Dependencies & Execution Order

### Phase Dependencies
- **Setup (Phase 1)**: No dependencies.
- **Foundational (Phase 2)**: Depends on Phase 1 - BLOCKS all user stories.
- **User Story 1 (Phase 3)**: Depends on Phase 2. Delivers MVP.
- **User Story 2 (Phase 4)**: Depends on Phase 3. Adds retry & failure handling.
- **User Story 3 (Phase 5)**: Depends on Phase 3. Adds multi-replica concurrency guarantees.
- **User Story 4 (Phase 6)**: Depends on Phase 3. Adds health server & graceful shutdown.
- **Polish (Phase 7)**: Depends on completion of user story phases.

### Parallel Opportunities
- Foundational tasks (T004, T005, T006, T009, T010) can be implemented in parallel.
- Unit test creation tasks across all phases are marked [P] and can be developed in parallel.
- Deployment artifacts (T033, T034, T036) can be created in parallel during Polish phase.

---

## Implementation Strategy

### MVP First (User Story 1 Only)
1. Complete Phase 1 (Setup) and Phase 2 (Foundational).
2. Complete Phase 3 (User Story 1).
3. Validate independent MVP execution: `./hros-event-worker run --type=directory` polling and publishing valid events.

### Incremental Delivery
1. Add User Story 2: Error classification, backoff, permanent failure marking (`FAILED`).
2. Add User Story 3: Multi-replica locking validation (`FOR UPDATE SKIP LOCKED`).
3. Add User Story 4: Graceful shutdown handler and HTTP health/metrics endpoints.
4. Finalize Docker container and Kubernetes deployment manifests.
