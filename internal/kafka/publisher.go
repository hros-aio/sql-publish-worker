package kafka

import (
	"context"

	"hros-event-worker/internal/event"
)

// Publisher defines methods for publishing serialized event envelopes to Kafka.
type Publisher interface {
	Publish(ctx context.Context, topic string, partitionKey string, envelope *event.Envelope) error
	Ping(ctx context.Context) error
	Close() error
}
