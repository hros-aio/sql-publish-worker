# Implementation Plan: HROS Outbox Kafka Relay Worker

**Branch**: `001-outbox-kafka-relay` | **Date**: 2026-09-28 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-outbox-kafka-relay/spec.md`

## Summary

Implement a reusable, high-performance Go worker binary (`hros-event-worker`) that polls PostgreSQL outbox tables for pending event records and reliably relays them to Apache Kafka with at-least-once delivery guarantees. The worker operates as a generic infrastructure engine supporting `setting`, `access`, and `directory` domains through Cobra CLI subcommands and isolated namespace configuration loading.

## Technical Context

**Language/Version**: Go 1.22+  
**Primary Dependencies**:
- `github.com/spf13/cobra` (CLI)
- `gopkg.in/yaml.v3` (Configuration parsing)
- `github.com/jackc/pgx/v5` & `database/sql` (PostgreSQL driver & connection pool)
- `github.com/IBM/sarama` (Kafka producer with `acks=all` and idempotent mode)
- `github.com/prometheus/client_golang/prometheus` (Prometheus metrics)
- `log/slog` (Standard library structured logging)
- `github.com/google/uuid` (UUID parsing & formatting)

**Storage**: PostgreSQL 14+ (Schema-per-service outbox tables)  
**Testing**: Go `testing`, `github.com/stretchr/testify` (unit & mock tests), Docker Compose/Testcontainers for end-to-end integration tests  
**Target Platform**: Linux (containerized, Kubernetes Deployments, scratch/distroless multi-stage image)  
**Project Type**: Standalone Go CLI & Background Worker Daemon  
**Performance Goals**: Sub-second polling throughput, bounded batch claim times, zero memory leaks across sustained long-running operation  
**Constraints**: Zero schema changes (strict adherence to existing `outbox_events` table); no business logic contamination; strict database-per-service isolation  
**Scale/Scope**: Supports multi-replica horizontal scaling per service type using PostgreSQL `FOR UPDATE SKIP LOCKED`

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle / Rule | Compliance Status | Rationale |
|------------------|-------------------|-----------|
| **Single Generic Binary** | PASS | Reusable outbox engine for `setting`, `access`, and `directory` |
| **No Cross-Service Access** | PASS | Database connections isolated strictly per selected worker `--type` |
| **At-Least-Once Delivery** | PASS | Kafka ACK required before DB status updated to `PUBLISHED` |
| **Idempotency Preservation**| PASS | Canonical `outbox_events.id` mapped directly to Kafka `eventId` |
| **Zero Migration Assumption**| PASS | Compliant with existing ENUM (`PENDING`, `PUBLISHED`, `FAILED`) and Option A in-memory retry |
| **Bounded DB Locking** | PASS | Short transactions with `FOR UPDATE SKIP LOCKED` |

---

## Project Structure

### Documentation (this feature)

```text
specs/001-outbox-kafka-relay/
├── plan.md              # Implementation plan
├── research.md          # Research & architectural decisions
├── data-model.md        # Data models & schemas
├── quickstart.md        # Validation & execution guide
├── contracts/           # CLI & Envelope contracts
│   ├── cli.md
│   └── event-envelope.md
├── checklists/
│   └── requirements.md
└── tasks.md             # Task breakdown (generated via /speckit-tasks)
```

### Source Code (repository root)

```text
cmd/
├── root.go                   # Root Cobra command
├── run.go                    # 'run --type=...' command
└── version.go                # Version & build info command

internal/
├── config/
│   ├── config.go             # Config structs & defaults
│   ├── loader.go             # YAML + ENV loader
│   └── validator.go          # Config validation rules
├── outbox/
│   ├── entity.go             # OutboxEvent model & status constants
│   ├── repository.go         # OutboxRepository interface
│   ├── postgres_repository.go# pgx/sql implementation (FOR UPDATE SKIP LOCKED)
│   └── processor.go          # Batch polling & lifecycle orchestration
├── kafka/
│   ├── publisher.go          # EventPublisher interface
│   ├── sarama_publisher.go   # IBM/sarama producer implementation
│   └── topic_resolver.go     # EventType to Kafka topic mapping
├── event/
│   ├── envelope.go           # Standard HROS event envelope
│   └── mapper.go             # Outbox to Envelope transformation
├── retry/
│   └── policy.go             # Exponential backoff, jitter, failure classifier
├── observability/
│   ├── logging.go            # slog initialization & JSON formatting
│   ├── metrics.go            # Prometheus metric registry & definitions
│   └── tracing.go            # OpenTelemetry / W3C TraceContext propagation
├── health/
│   └── server.go             # HTTP healthz/readyz and metrics server
└── worker/
    └── runner.go             # Worker lifecycle supervisor & signal handling

deployments/
├── docker/
│   └── Dockerfile            # Multi-stage Go build
└── k8s/
    ├── setting-deployment.yaml
    ├── access-deployment.yaml
    └── directory-deployment.yaml

config.example.yaml           # Example YAML configuration
main.go                       # Application entrypoint
go.mod
go.sum
```

**Structure Decision**: Clean internal package layout separating generic outbox domain logic, Kafka publisher adapters, PostgreSQL repository access, and CLI command bindings.

---

## Complexity Tracking

No constitution violations detected. Standard enterprise Go worker architecture with clean boundaries.
