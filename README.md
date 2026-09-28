# HROS Outbox Kafka Relay Worker

[![Go Version](https://img.shields.io/badge/go-1.22+-blue.svg)](https://golang.org)
[![Status](https://img.shields.io/badge/status-production--ready-brightgreen.svg)]()

High-performance, generic Golang background worker responsible for reading pending transactional records from PostgreSQL Outbox tables and publishing them to Apache Kafka across HROS service domains (`setting`, `access`, and `directory`).

---

## Key Features

- **Single Reusable Binary**: Unified codebase with dynamic domain configuration selection via Cobra CLI (`--type=directory`).
- **Database-per-Service Isolation**: Strict domain isolation without cross-service database access.
- **At-Least-Once Delivery**: Guaranteed broker acknowledgement (`acks=all`) before marking records `PUBLISHED`.
- **Concurrency & Scaling**: Safe horizontal scaling using PostgreSQL row-level locking (`FOR UPDATE SKIP LOCKED`).
- **Failure Isolation & Retry**: In-memory exponential backoff with full jitter for transient broker errors, marking unrecoverable records as `FAILED` without blocking the batch pipeline.
- **Observability**: Structured JSON logging (`slog`), Prometheus metrics (`/metrics`), and Kubernetes health probes (`/healthz`, `/readyz`).

---

## Quickstart

### 1. Build Binary

```bash
go build -o hros-event-worker main.go
```

### 2. View CLI Help & Version

```bash
./hros-event-worker --help
./hros-event-worker version
```

### 3. Run Worker for a Service Domain

```bash
# Directory Service Worker
./hros-event-worker run --type=directory --config=./config.yaml

# Setting Service Worker
./hros-event-worker run --type=setting --config=./config.yaml

# Access Service Worker
./hros-event-worker run --type=access --config=./config.yaml
```

---

## Configuration

Configuration is loaded from a YAML configuration file and can be overridden via environment variables.

### Priority
```text
Environment Variables  (Highest)
       ↓
YAML Configuration File
       ↓
Built-in Defaults      (Lowest)
```

### Example `config.yaml`
```yaml
directory:
  database:
    host: localhost
    port: 5432
    name: hros_directory
    user: postgres
    password: postgres
    ssl_mode: disable
  kafka:
    brokers:
      - localhost:9092
    client_id: hros-directory-event-worker
    topic_prefix: ""
  outbox:
    batch_size: 100
    poll_interval: 1s
    max_retries: 5
    retry_backoff: 500ms
  worker:
    concurrency: 2
    shutdown_timeout: 15s
  http:
    port: 8080
```

### Environment Variable Overrides
All configuration parameters can be overridden using the prefix `HROS_<TYPE>_*`:
```bash
export HROS_DIRECTORY_DATABASE_HOST=postgres.directory.svc.cluster.local
export HROS_DIRECTORY_DATABASE_PASSWORD=secret_password
export HROS_DIRECTORY_KAFKA_BROKERS=kafka-broker:9092
export HROS_DIRECTORY_OUTBOX_BATCH_SIZE=200
```

---

## Docker & Kubernetes Deployment

### Docker Build
```bash
docker build -t hros/event-worker:v1.0.0 -f deployments/docker/Dockerfile .
```

### Kubernetes Deployments
Deploy the same image across all three service domains:
```bash
kubectl apply -f deployments/k8s/setting-deployment.yaml
kubectl apply -f deployments/k8s/access-deployment.yaml
kubectl apply -f deployments/k8s/directory-deployment.yaml
```

---

## Architecture & Design Decisions
See the Architecture Decision Record: [`docs/adr/0001-outbox-kafka-relay.md`](./docs/adr/0001-outbox-kafka-relay.md).
