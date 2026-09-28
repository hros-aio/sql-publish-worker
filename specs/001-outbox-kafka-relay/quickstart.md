# Quickstart & Validation Guide: Outbox Kafka Relay Worker

This document provides instructions for running and validating the outbox relay worker across `setting`, `access`, and `directory` domains.

---

## 1. Prerequisites

- Go 1.22+ installed
- Docker & Docker Compose (for local PostgreSQL & Kafka test environment)

---

## 2. Local Infrastructure Setup

Start PostgreSQL and Kafka test instances:

```bash
docker compose up -d postgres kafka
```

Apply the outbox schema to the test database:

```sql
CREATE TYPE outbox_status AS ENUM ('PENDING', 'PUBLISHED', 'FAILED');

CREATE TABLE outbox_events (
    id              uuid         NOT NULL DEFAULT gen_random_uuid(),
    tenant_code     varchar(64)  NOT NULL,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    updated_at      timestamptz  NOT NULL DEFAULT now(),
    aggregate_type  varchar(64)  NOT NULL,
    aggregate_id    uuid         NOT NULL,
    event_type      varchar(128) NOT NULL,
    event_version   int          NOT NULL DEFAULT 1,
    payload         jsonb        NOT NULL,
    status          outbox_status NOT NULL DEFAULT 'PENDING',
    published_at    timestamptz  NULL,
    CONSTRAINT pk_outbox_events PRIMARY KEY (id),
    CONSTRAINT uq_outbox_events_tenant_id UNIQUE (tenant_code, id)
);

CREATE INDEX idx_outbox_events_status_created ON outbox_events (tenant_code, status, created_at);
```

---

## 3. Configuration

Create or verify `config.yaml`:

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
    batch_size: 50
    poll_interval: 1s
    max_retries: 3
    retry_backoff: 500ms
  worker:
    concurrency: 2
    shutdown_timeout: 10s
  http:
    port: 8080
```

---

## 4. Running the Worker

Build and start the worker for the `directory` service:

```bash
go build -o hros-event-worker main.go
./hros-event-worker run --type=directory --config=./config.yaml
```

---

## 5. End-to-End Validation Scenario

### Step 1: Insert Pending Event
```sql
INSERT INTO outbox_events (
    tenant_code,
    aggregate_type,
    aggregate_id,
    event_type,
    event_version,
    payload,
    status
) VALUES (
    'tenant-alpha',
    'employee',
    'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d',
    'directory.employee.created',
    1,
    '{"id":"a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d","name":"Alice"}'::jsonb,
    'PENDING'
);
```

### Step 2: Observe Worker Logs & Metrics
The worker will log:
```json
{"level":"info","message":"outbox event published","worker_type":"directory","event_id":"...","topic":"directory.employee"}
```

### Step 3: Verify Database Status
```sql
SELECT id, status, published_at FROM outbox_events WHERE aggregate_type = 'employee';
```
*Expected Outcome*: `status` is `PUBLISHED` and `published_at` is populated.

### Step 4: Verify Kafka Topic
Consume the message using Kafka CLI:
```bash
kcat -b localhost:9092 -C -t directory.employee -J
```
*Expected Outcome*: JSON envelope containing original `eventId`, `tenantCode: tenant-alpha`, and intact payload.
