package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Envelope represents the canonical HROS event envelope structure published to Kafka.
type Envelope struct {
	EventID       uuid.UUID       `json:"eventId"`
	EventType     string          `json:"eventType"`
	EventVersion  int             `json:"eventVersion"`
	TenantCode    string          `json:"tenantCode"`
	OccurredAt    time.Time       `json:"occurredAt"`
	Producer      string          `json:"producer"`
	CorrelationID *string         `json:"correlationId"`
	CausationID   *string         `json:"causationId"`
	TraceID       *string         `json:"traceId"`
	Payload       json.RawMessage `json:"payload"`
}
