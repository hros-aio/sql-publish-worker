package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"hros-event-worker/internal/config"
	"hros-event-worker/internal/event"
	"hros-event-worker/internal/kafka"
	"hros-event-worker/internal/outbox"
)

type InMemoryPublisher struct {
	PublishedMessages []PublishedMessage
	ShouldFail        bool
}

type PublishedMessage struct {
	Topic        string
	PartitionKey string
	Envelope     *event.Envelope
}

func (p *InMemoryPublisher) Publish(ctx context.Context, topic string, partitionKey string, envelope *event.Envelope) error {
	if p.ShouldFail {
		return assert.AnError
	}
	p.PublishedMessages = append(p.PublishedMessages, PublishedMessage{
		Topic:        topic,
		PartitionKey: partitionKey,
		Envelope:     envelope,
	})
	return nil
}

func (p *InMemoryPublisher) Ping(ctx context.Context) error {
	if p.ShouldFail {
		return assert.AnError
	}
	return nil
}

func (p *InMemoryPublisher) Close() error {
	return nil
}

// InMemoryRepository for testing complete lifecycle
type InMemoryRepository struct {
	events map[uuid.UUID]*outbox.Event
}

func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		events: make(map[uuid.UUID]*outbox.Event),
	}
}

func (r *InMemoryRepository) AddEvent(evt *outbox.Event) {
	r.events[evt.ID] = evt
}

func (r *InMemoryRepository) ClaimBatch(ctx context.Context, limit int) ([]outbox.Event, error) {
	var result []outbox.Event
	for _, evt := range r.events {
		if evt.Status == outbox.StatusPending {
			result = append(result, *evt)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (r *InMemoryRepository) MarkPublished(ctx context.Context, ids []uuid.UUID, publishedAt time.Time) error {
	for _, id := range ids {
		if evt, ok := r.events[id]; ok {
			evt.Status = outbox.StatusPublished
			evt.PublishedAt = &publishedAt
		}
	}
	return nil
}

func (r *InMemoryRepository) MarkFailed(ctx context.Context, id uuid.UUID, failureErr error) error {
	if evt, ok := r.events[id]; ok {
		evt.Status = outbox.StatusFailed
	}
	return nil
}

func (r *InMemoryRepository) Ping(ctx context.Context) error {
	return nil
}

func (r *InMemoryRepository) Close() error {
	return nil
}

func TestEndToEndRelayLifecycle(t *testing.T) {
	repo := NewInMemoryRepository()
	pub := &InMemoryPublisher{}
	resolver := kafka.NewTopicResolver("hros")

	eventID := uuid.New()
	aggID := uuid.New()
	testEvent := &outbox.Event{
		ID:            eventID,
		TenantCode:    "tenant-enterprise",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
		AggregateType: "employee",
		AggregateID:   aggID,
		EventType:     "directory.employee.created",
		EventVersion:  1,
		Payload:       []byte(`{"name":"Charlie","department":"IT"}`),
		Status:        outbox.StatusPending,
	}
	repo.AddEvent(testEvent)

	cfg := config.DefaultDomainConfig("directory")
	processor := outbox.NewProcessor("directory", cfg, repo, pub, resolver, nil, nil)

	// Process batch
	count, err := processor.ProcessBatch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify Kafka publish
	require.Len(t, pub.PublishedMessages, 1)
	msg := pub.PublishedMessages[0]
	assert.Equal(t, "hros.directory.employee", msg.Topic)
	assert.Equal(t, aggID.String(), msg.PartitionKey)
	assert.Equal(t, eventID, msg.Envelope.EventID)
	assert.Equal(t, "tenant-enterprise", msg.Envelope.TenantCode)

	// Verify database status transition
	assert.Equal(t, outbox.StatusPublished, repo.events[eventID].Status)
	assert.NotNil(t, repo.events[eventID].PublishedAt)
}

func TestEndToEndRelayFailureLifecycle(t *testing.T) {
	repo := NewInMemoryRepository()
	pub := &InMemoryPublisher{ShouldFail: true}
	resolver := kafka.NewTopicResolver("")

	eventID := uuid.New()
	aggID := uuid.New()
	testEvent := &outbox.Event{
		ID:            eventID,
		TenantCode:    "tenant-enterprise",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
		AggregateType: "company",
		AggregateID:   aggID,
		EventType:     "setting.company.updated",
		EventVersion:  1,
		Payload:       []byte(`{"name":"Acme Corp"}`),
		Status:        outbox.StatusPending,
	}
	repo.AddEvent(testEvent)

	cfg := config.DefaultDomainConfig("setting")
	cfg.Outbox.MaxRetries = 1
	processor := outbox.NewProcessor("setting", cfg, repo, pub, resolver, nil, nil)

	count, err := processor.ProcessBatch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify database status remains PENDING for transient failures
	assert.Equal(t, outbox.StatusPending, repo.events[eventID].Status)
	assert.Nil(t, repo.events[eventID].PublishedAt)
}

func TestEndToEndRelayPermanentFailureLifecycle(t *testing.T) {
	repo := NewInMemoryRepository()
	pub := &InMemoryPublisher{}
	resolver := kafka.NewTopicResolver("")

	eventID := uuid.New()
	aggID := uuid.New()
	testEvent := &outbox.Event{
		ID:            eventID,
		TenantCode:    "tenant-enterprise",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
		AggregateType: "company",
		AggregateID:   aggID,
		EventType:     "setting.company.updated",
		EventVersion:  1,
		Payload:       []byte(`{malformed json`),
		Status:        outbox.StatusPending,
	}
	repo.AddEvent(testEvent)

	cfg := config.DefaultDomainConfig("setting")
	processor := outbox.NewProcessor("setting", cfg, repo, pub, resolver, nil, nil)

	count, err := processor.ProcessBatch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify database status transition to FAILED for permanent error
	assert.Equal(t, outbox.StatusFailed, repo.events[eventID].Status)
	assert.Nil(t, repo.events[eventID].PublishedAt)
}

// Dummy check to ensure sql package is referenced
var _ = sql.ErrNoRows
