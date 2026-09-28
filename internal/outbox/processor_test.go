package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"hros-event-worker/internal/config"
	"hros-event-worker/internal/event"
)

// MockRepository
type MockRepository struct {
	mock.Mock
}

func (m *MockRepository) ClaimBatch(ctx context.Context, limit int) ([]Event, error) {
	args := m.Called(ctx, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]Event), args.Error(1)
}

func (m *MockRepository) MarkPublished(ctx context.Context, ids []uuid.UUID, publishedAt time.Time) error {
	args := m.Called(ctx, ids, publishedAt)
	return args.Error(0)
}

func (m *MockRepository) MarkFailed(ctx context.Context, id uuid.UUID, failureErr error) error {
	args := m.Called(ctx, id, failureErr)
	return args.Error(0)
}

func (m *MockRepository) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockRepository) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockPublisher
type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) Publish(ctx context.Context, topic string, partitionKey string, envelope *event.Envelope) error {
	args := m.Called(ctx, topic, partitionKey, envelope)
	return args.Error(0)
}

func (m *MockPublisher) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockTopicResolver
type MockTopicResolver struct {
	mock.Mock
}

func (m *MockTopicResolver) ResolveTopic(eventType string) (string, error) {
	args := m.Called(eventType)
	return args.String(0), args.Error(1)
}

func TestProcessor_ProcessBatch_Success(t *testing.T) {
	cfg := config.DefaultDomainConfig("directory")
	cfg.Outbox.MaxRetries = 2
	cfg.Outbox.RetryBackoff = 10 * time.Millisecond

	mockRepo := new(MockRepository)
	mockPublisher := new(MockPublisher)
	mockResolver := new(MockTopicResolver)

	eventID := uuid.New()
	aggregateID := uuid.New()
	events := []Event{
		{
			ID:            eventID,
			TenantCode:    "tenant-1",
			CreatedAt:     time.Now().UTC(),
			AggregateType: "employee",
			AggregateID:   aggregateID,
			EventType:     "directory.employee.created",
			EventVersion:  1,
			Payload:       []byte(`{"name":"Bob"}`),
			Status:        StatusPending,
		},
	}

	mockRepo.On("ClaimBatch", mock.Anything, cfg.Outbox.BatchSize).Return(events, nil)
	mockResolver.On("ResolveTopic", "directory.employee.created").Return("directory.employee", nil)
	mockPublisher.On("Publish", mock.Anything, "directory.employee", aggregateID.String(), mock.Anything).Return(nil)
	mockRepo.On("MarkPublished", mock.Anything, []uuid.UUID{eventID}, mock.Anything).Return(nil)

	processor := NewProcessor("directory", cfg, mockRepo, mockPublisher, mockResolver, nil, nil)

	count, err := processor.ProcessBatch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	mockRepo.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
	mockResolver.AssertExpectations(t)
}

func TestProcessor_ProcessBatch_FailureIsolation(t *testing.T) {
	cfg := config.DefaultDomainConfig("directory")
	cfg.Outbox.MaxRetries = 1

	mockRepo := new(MockRepository)
	mockPublisher := new(MockPublisher)
	mockResolver := new(MockTopicResolver)

	validEventID := uuid.New()
	validAggID := uuid.New()
	badEventID := uuid.New()

	events := []Event{
		{
			ID:         badEventID,
			TenantCode: "tenant-1",
			EventType:  "directory.employee.created",
			Payload:    []byte(`{bad json`),
			Status:     StatusPending,
		},
		{
			ID:            validEventID,
			TenantCode:    "tenant-1",
			CreatedAt:     time.Now().UTC(),
			AggregateType: "employee",
			AggregateID:   validAggID,
			EventType:     "directory.employee.created",
			EventVersion:  1,
			Payload:       []byte(`{"name":"Alice"}`),
			Status:        StatusPending,
		},
	}

	mockRepo.On("ClaimBatch", mock.Anything, cfg.Outbox.BatchSize).Return(events, nil)
	// Bad event fails mapping and is marked FAILED in DB
	mockRepo.On("MarkFailed", mock.Anything, badEventID, mock.Anything).Return(nil)

	// Valid event succeeds
	mockResolver.On("ResolveTopic", "directory.employee.created").Return("directory.employee", nil)
	mockPublisher.On("Publish", mock.Anything, "directory.employee", validAggID.String(), mock.Anything).Return(nil)
	mockRepo.On("MarkPublished", mock.Anything, []uuid.UUID{validEventID}, mock.Anything).Return(nil)

	processor := NewProcessor("directory", cfg, mockRepo, mockPublisher, mockResolver, nil, nil)

	count, err := processor.ProcessBatch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	mockRepo.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestProcessor_ProcessBatch_KafkaRetryExhaustion(t *testing.T) {
	cfg := config.DefaultDomainConfig("access")
	cfg.Outbox.MaxRetries = 2
	cfg.Outbox.RetryBackoff = 1 * time.Millisecond

	mockRepo := new(MockRepository)
	mockPublisher := new(MockPublisher)
	mockResolver := new(MockTopicResolver)

	eventID := uuid.New()
	aggID := uuid.New()
	events := []Event{
		{
			ID:            eventID,
			TenantCode:    "tenant-1",
			CreatedAt:     time.Now().UTC(),
			AggregateType: "role",
			AggregateID:   aggID,
			EventType:     "access.role.assigned",
			EventVersion:  1,
			Payload:       []byte(`{"role":"admin"}`),
			Status:        StatusPending,
		},
	}

	mockRepo.On("ClaimBatch", mock.Anything, cfg.Outbox.BatchSize).Return(events, nil)
	mockResolver.On("ResolveTopic", "access.role.assigned").Return("access.role", nil)
	// Kafka fails twice (transient timeout)
	mockPublisher.On("Publish", mock.Anything, "access.role", aggID.String(), mock.Anything).
		Return(errors.New("connection reset by peer")).Twice()

	mockRepo.On("MarkFailed", mock.Anything, eventID, mock.Anything).Return(nil)

	processor := NewProcessor("access", cfg, mockRepo, mockPublisher, mockResolver, nil, nil)

	count, err := processor.ProcessBatch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	mockRepo.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}
