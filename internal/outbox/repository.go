package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Repository defines data access methods for the PostgreSQL outbox_events table.
type Repository interface {
	ClaimBatch(ctx context.Context, limit int) ([]Event, error)
	MarkPublished(ctx context.Context, ids []uuid.UUID, publishedAt time.Time) error
	MarkFailed(ctx context.Context, id uuid.UUID, failureErr error) error
	Ping(ctx context.Context) error
	Close() error
}
