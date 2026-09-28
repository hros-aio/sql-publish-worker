# Data Model: Outbox Kafka Relay Worker

**Feature**: Outbox Kafka Relay Worker  
**Date**: 2026-09-28  
**Spec**: [spec.md](./spec.md)

---

## 1. Outbox Event Entity (`outbox_events` table)

### Schema Definition

```sql
CREATE TYPE outbox_status AS ENUM (
    'PENDING',
    'PUBLISHED',
    'FAILED'
);

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
CREATE INDEX idx_outbox_events_aggregate ON outbox_events (tenant_code, aggregate_type, aggregate_id);
```

### Go Domain Model

```go
package outbox

import (
    "time"
    "github.com/google/uuid"
)

type Status string

const (
    StatusPending   Status = "PENDING"
    StatusPublished Status = "PUBLISHED"
    StatusFailed    Status = "FAILED"
)

type Event struct {
    ID            uuid.UUID       `json:"id" db:"id"`
    TenantCode    string          `json:"tenant_code" db:"tenant_code"`
    CreatedAt     time.Time       `json:"created_at" db:"created_at"`
    UpdatedAt     time.Time       `json:"updated_at" db:"updated_at"`
    AggregateType string          `json:"aggregate_type" db:"aggregate_type"`
    AggregateID   uuid.UUID       `json:"aggregate_id" db:"aggregate_id"`
    EventType     string          `json:"event_type" db:"event_type"`
    EventVersion  int             `json:"event_version" db:"event_version"`
    Payload       []byte          `json:"payload" db:"payload"`
    Status        Status          `json:"status" db:"status"`
    PublishedAt   *time.Time      `json:"published_at,omitempty" db:"published_at"`
}
```

### State Transitions

```text
[Transaction Commit in Domain Service]
                 │
                 ▼
             (PENDING)
                 │
        ┌────────┴────────┐
        │                 │
 (Kafka ACK)      (Retries Exhausted / Invalid)
        │                 │
        ▼                 ▼
  (PUBLISHED)          (FAILED)
```

- **PENDING**: Initial state inserted by domain service transaction. Eligible for worker batch claim.
- **PUBLISHED**: Successfully acknowledged by Kafka broker with timestamp recorded in `published_at`.
- **FAILED**: Permanent deserialization failure or max retry limit reached.

---

## 2. Kafka Standard Event Envelope

```go
package event

import (
    "encoding/json"
    "time"
    "github.com/google/uuid"
)

type Envelope struct {
    EventID       uuid.UUID       `json:"eventId"`
    EventType     string          `json:"eventType"`
    EventVersion  int             `json:"eventVersion"`
    TenantCode    string          `json:"tenantCode"`
    OccurredAt    time.Time       `json:"occurredAt"`
    Producer      string          `json:"producer"`
    CorrelationID *string         `json:"correlationId,omitempty"`
    CausationID   *string         `json:"causationId,omitempty"`
    TraceID       *string         `json:"traceId,omitempty"`
    Payload       json.RawMessage `json:"payload"`
}
```

### Validation Rules

1. `EventID` MUST equal `outbox_events.id`.
2. `TenantCode` MUST equal `outbox_events.tenant_code`.
3. `EventType` MUST NOT be empty.
4. `Payload` MUST be valid JSON data.
5. Partition key MUST be `aggregate_id.String()` or `tenant_code + ":" + aggregate_id.String()`.

---

## 3. Configuration Model

```go
package config

import "time"

type RootConfig struct {
    Setting   DomainConfig `yaml:"setting"`
    Access    DomainConfig `yaml:"access"`
    Directory DomainConfig `yaml:"directory"`
}

type DomainConfig struct {
    Database DatabaseConfig `yaml:"database"`
    Kafka    KafkaConfig    `yaml:"kafka"`
    Outbox   OutboxConfig   `yaml:"outbox"`
    Worker   WorkerConfig   `yaml:"worker"`
    HTTP     HTTPConfig     `yaml:"http"`
}

type DatabaseConfig struct {
    Host            string        `yaml:"host"`
    Port            int           `yaml:"port"`
    Name            string        `yaml:"name"`
    User            string        `yaml:"user"`
    Password        string        `yaml:"password"`
    SSLMode         string        `yaml:"ssl_mode"`
    MaxOpenConns    int           `yaml:"max_open_conns"`
    MaxIdleConns    int           `yaml:"max_idle_conns"`
    ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

type KafkaConfig struct {
    Brokers       []string      `yaml:"brokers"`
    ClientID      string        `yaml:"client_id"`
    TopicPrefix   string        `yaml:"topic_prefix"`
    RequiredAcks  string        `yaml:"required_acks"`
    RetryMax      int           `yaml:"retry_max"`
    RetryBackoff  time.Duration `yaml:"retry_backoff"`
    Compression   string        `yaml:"compression"`
    ProducerName  string        `yaml:"producer_name"`
}

type OutboxConfig struct {
    BatchSize         int           `yaml:"batch_size"`
    PollInterval      time.Duration `yaml:"poll_interval"`
    MaxRetries        int           `yaml:"max_retries"`
    RetryBackoff      time.Duration `yaml:"retry_backoff"`
    ClaimTimeout      time.Duration `yaml:"claim_timeout"`
    ProcessingTimeout time.Duration `yaml:"processing_timeout"`
}

type WorkerConfig struct {
    Concurrency     int           `yaml:"concurrency"`
    ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type HTTPConfig struct {
    Port int `yaml:"port"`
}
```
