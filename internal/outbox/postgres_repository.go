package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"hros-event-worker/internal/config"
)

type PostgresRepository struct {
	db *sql.DB
}

// NewPostgresRepository initializes a connection pool to the PostgreSQL database.
func NewPostgresRepository(cfg config.DatabaseConfig) (*PostgresRepository, error) {
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Name, cfg.User, cfg.Password, sslMode)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database handle: %w", err)
	}

	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgresRepository{db: db}, nil
}

// NewPostgresRepositoryWithDB allows creating a repository from an existing *sql.DB (for testing).
func NewPostgresRepositoryWithDB(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

// ClaimBatch queries and claims up to limit PENDING records atomically using FOR UPDATE SKIP LOCKED.
func (r *PostgresRepository) ClaimBatch(ctx context.Context, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 100
	}

	// Use short transaction to lock and extract claimed rows
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	query := `
		SELECT
			id,
			tenant_code,
			created_at,
			updated_at,
			aggregate_type,
			aggregate_id,
			event_type,
			event_version,
			payload,
			status,
			published_at
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED;
	`

	rows, err := tx.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var evt Event
		var payloadBytes []byte
		var publishedAt sql.NullTime

		err := rows.Scan(
			&evt.ID,
			&evt.TenantCode,
			&evt.CreatedAt,
			&evt.UpdatedAt,
			&evt.AggregateType,
			&evt.AggregateID,
			&evt.EventType,
			&evt.EventVersion,
			&payloadBytes,
			&evt.Status,
			&publishedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan outbox event row: %w", err)
		}

		evt.Payload = payloadBytes
		if publishedAt.Valid {
			evt.PublishedAt = &publishedAt.Time
		}

		events = append(events, evt)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during rows iteration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit claim transaction: %w", err)
	}

	return events, nil
}

// MarkPublished marks a batch of event IDs as PUBLISHED.
func (r *PostgresRepository) MarkPublished(ctx context.Context, ids []uuid.UUID, publishedAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}

	query := `
		UPDATE outbox_events
		SET status = 'PUBLISHED',
		    published_at = $1,
		    updated_at = now()
		WHERE id = ANY($2::uuid[])
	`

	// Convert []uuid.UUID to string representation for array param
	idStrings := make([]string, len(ids))
	for i, id := range ids {
		idStrings[i] = id.String()
	}

	_, err := r.db.ExecContext(ctx, query, publishedAt.UTC(), fmt.Sprintf("{%s}", joinStrings(idStrings, ",")))
	if err != nil {
		return fmt.Errorf("failed to mark events as published: %w", err)
	}

	return nil
}

// MarkFailed marks an event ID as permanently FAILED.
func (r *PostgresRepository) MarkFailed(ctx context.Context, id uuid.UUID, failureErr error) error {
	query := `
		UPDATE outbox_events
		SET status = 'FAILED',
		    updated_at = now()
		WHERE id = $1
	`

	_, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to mark event %s as failed: %w", id, err)
	}

	return nil
}

func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

func (r *PostgresRepository) Close() error {
	return r.db.Close()
}

func joinStrings(elems []string, sep string) string {
	switch len(elems) {
	case 0:
		return ""
	case 1:
		return elems[0]
	}
	n := len(sep) * (len(elems) - 1)
	for i := 0; i < len(elems); i++ {
		n += len(elems[i])
	}

	var b []byte
	b = append(b, elems[0]...)
	for _, s := range elems[1:] {
		b = append(b, sep...)
		b = append(b, s...)
	}
	return string(b)
}
