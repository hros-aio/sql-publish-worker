package event

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// MapToEnvelope converts raw outbox event fields into a standard HROS Event Envelope.
func MapToEnvelope(
	id uuid.UUID,
	tenantCode string,
	eventType string,
	eventVersion int,
	createdAt time.Time,
	payload []byte,
	producerName string,
) (*Envelope, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("event id cannot be nil")
	}

	// Validate payload is valid JSON
	var raw json.RawMessage
	if len(payload) == 0 {
		raw = json.RawMessage("{}")
	} else {
		if !json.Valid(payload) {
			return nil, fmt.Errorf("invalid json payload for event id: %s", id.String())
		}
		raw = json.RawMessage(payload)
	}

	if producerName == "" {
		producerName = "hros-" + tenantCode + "-service"
	}

	envelope := &Envelope{
		EventID:       id,
		EventType:     eventType,
		EventVersion:  eventVersion,
		TenantCode:    tenantCode,
		OccurredAt:    createdAt.UTC(),
		Producer:      producerName,
		CorrelationID: nil,
		CausationID:   nil,
		TraceID:       nil,
		Payload:       raw,
	}

	return envelope, nil
}
