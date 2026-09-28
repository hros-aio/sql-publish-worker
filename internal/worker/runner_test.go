package worker

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"hros-event-worker/internal/config"
	"hros-event-worker/internal/outbox"
)

type mockRepo struct {
	mock.Mock
}

func (m *mockRepo) ClaimBatch(ctx context.Context, limit int) ([]outbox.Event, error) {
	args := m.Called(ctx, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]outbox.Event), args.Error(1)
}

func (m *mockRepo) MarkPublished(ctx context.Context, ids []uuid.UUID, publishedAt time.Time) error {
	return nil
}

func (m *mockRepo) MarkFailed(ctx context.Context, id uuid.UUID, failureErr error) error {
	return nil
}

func (m *mockRepo) Ping(ctx context.Context) error {
	return nil
}

func (m *mockRepo) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockPub struct {
	mock.Mock
}

func (m *mockPub) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestRunner_GracefulCancel(t *testing.T) {
	cfg := config.DefaultDomainConfig("directory")
	cfg.Worker.Concurrency = 1
	cfg.Worker.ShutdownTimeout = 2 * time.Second
	cfg.Outbox.PollInterval = 50 * time.Millisecond

	repo := new(mockRepo)
	repo.On("ClaimBatch", mock.Anything, mock.Anything).Return([]outbox.Event{}, nil)
	repo.On("Close").Return(nil)

	pub := new(mockPub)
	pub.On("Close").Return(nil)

	// Outbox processor
	processor := outbox.NewProcessor("directory", cfg, repo, nil, nil, nil, nil)

	runner := NewRunner("directory", &cfg, repo, nil, processor, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := runner.Run(ctx)
	assert.NoError(t, err)
}
