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

// Event represents an individual record in the outbox_events table.
type Event struct {
	ID            uuid.UUID  `json:"id" db:"id"`
	TenantCode    string     `json:"tenant_code" db:"tenant_code"`
	CreatedAt     time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at" db:"updated_at"`
	AggregateType string     `json:"aggregate_type" db:"aggregate_type"`
	AggregateID   uuid.UUID  `json:"aggregate_id" db:"aggregate_id"`
	EventType     string     `json:"event_type" db:"event_type"`
	EventVersion  int        `json:"event_version" db:"event_version"`
	Payload       []byte     `json:"payload" db:"payload"`
	Status        Status     `json:"status" db:"status"`
	PublishedAt   *time.Time `json:"published_at,omitempty" db:"published_at"`
}
