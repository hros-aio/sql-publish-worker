package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"hros-event-worker/internal/event"
	"hros-event-worker/internal/outbox"
)

type mockRepo struct {
	mock.Mock
}

func (m *mockRepo) ClaimBatch(ctx context.Context, limit int) ([]outbox.Event, error) {
	return nil, nil
}
func (m *mockRepo) MarkPublished(ctx context.Context, ids []uuid.UUID, publishedAt time.Time) error {
	return nil
}
func (m *mockRepo) MarkFailed(ctx context.Context, id uuid.UUID, failureErr error) error {
	return nil
}
func (m *mockRepo) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
func (m *mockRepo) Close() error {
	return nil
}

type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) Publish(ctx context.Context, topic string, partitionKey string, env *event.Envelope) error {
	return nil
}
func (m *mockPublisher) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
func (m *mockPublisher) Close() error {
	return nil
}

func TestHealthServer_Liveness(t *testing.T) {
	srv := NewServer(8080, nil, nil)
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	srv.handleLiveness(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"status":"UP"`)
}

func TestHealthServer_Readiness_Healthy(t *testing.T) {
	repo := new(mockRepo)
	pub := new(mockPublisher)

	repo.On("Ping", mock.Anything).Return(nil)
	pub.On("Ping", mock.Anything).Return(nil)

	srv := NewServer(8080, repo, pub)
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()

	srv.handleReadiness(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"status":"READY"`)
}

func TestHealthServer_Readiness_Unhealthy(t *testing.T) {
	repo := new(mockRepo)
	pub := new(mockPublisher)

	repo.On("Ping", mock.Anything).Return(errors.New("db connection timeout"))

	srv := NewServer(8080, repo, pub)
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()

	srv.handleReadiness(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Contains(t, w.Body.String(), `database unreachable`)
}
