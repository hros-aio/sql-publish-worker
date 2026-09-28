package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"hros-event-worker/internal/config"
	"hros-event-worker/internal/event"
	"hros-event-worker/internal/kafka"
	"hros-event-worker/internal/observability"
	"hros-event-worker/internal/retry"
)

// Processor coordinates polling outbox records, mapping to envelopes, publishing to Kafka, and updating DB status.
type Processor struct {
	workerType    string
	cfg           config.DomainConfig
	repo          Repository
	publisher     kafka.Publisher
	topicResolver kafka.TopicResolver
	retryPolicy   *retry.Policy
	metrics       *observability.Metrics
	logger        *slog.Logger
}

func NewProcessor(
	workerType string,
	cfg config.DomainConfig,
	repo Repository,
	publisher kafka.Publisher,
	topicResolver kafka.TopicResolver,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *Processor {
	if metrics == nil {
		metrics = observability.DefaultMetrics
	}
	if logger == nil {
		logger = slog.Default()
	}

	retryPolicy := retry.NewPolicy(cfg.Outbox.MaxRetries, cfg.Outbox.RetryBackoff)

	return &Processor{
		workerType:    workerType,
		cfg:           cfg,
		repo:          repo,
		publisher:     publisher,
		topicResolver: topicResolver,
		retryPolicy:   retryPolicy,
		metrics:       metrics,
		logger:        logger,
	}
}

// ProcessBatch claims a batch of events and processes them. Returns the number of events processed.
func (p *Processor) ProcessBatch(ctx context.Context) (int, error) {
	start := time.Now()

	if p.metrics != nil && p.metrics.PollTotal != nil {
		p.metrics.PollTotal.WithLabelValues(p.workerType).Inc()
	}

	claimCtx, cancelClaim := context.WithTimeout(ctx, p.cfg.Outbox.ClaimTimeout)
	events, err := p.repo.ClaimBatch(claimCtx, p.cfg.Outbox.BatchSize)
	cancelClaim()

	if err != nil {
		if p.metrics != nil && p.metrics.PollErrorsTotal != nil {
			p.metrics.PollErrorsTotal.WithLabelValues(p.workerType).Inc()
		}
		return 0, fmt.Errorf("failed to claim outbox batch: %w", err)
	}

	count := len(events)
	if count == 0 {
		return 0, nil
	}

	if p.metrics != nil {
		if p.metrics.EventsClaimedTotal != nil {
			p.metrics.EventsClaimedTotal.WithLabelValues(p.workerType).Add(float64(count))
		}
		if p.metrics.BatchSize != nil {
			p.metrics.BatchSize.WithLabelValues(p.workerType).Set(float64(count))
		}
	}

	var publishedIDs []uuid.UUID

	for _, evt := range events {
		select {
		case <-ctx.Done():
			p.logger.Warn("Batch processing cancelled by context",
				"worker_type", p.workerType,
				"processed_count", len(publishedIDs),
				"remaining_count", count-len(publishedIDs),
			)
			// Mark what we have successfully published so far
			if len(publishedIDs) > 0 {
				_ = p.repo.MarkPublished(context.Background(), publishedIDs, time.Now().UTC())
			}
			return len(publishedIDs), ctx.Err()
		default:
		}

		// 1. Map to Envelope
		envelope, err := event.MapToEnvelope(
			evt.ID,
			evt.TenantCode,
			evt.EventType,
			evt.EventVersion,
			evt.CreatedAt,
			evt.Payload,
			p.cfg.Kafka.ProducerName,
		)
		if err != nil {
			p.logger.Error("Failed to map outbox event to envelope, marking as FAILED",
				"worker_type", p.workerType,
				"event_id", evt.ID.String(),
				"tenant_code", evt.TenantCode,
				"event_type", evt.EventType,
				"error", err.Error(),
			)
			if markErr := p.repo.MarkFailed(ctx, evt.ID, err); markErr != nil {
				p.logger.Error("Failed to mark event as FAILED in database", "event_id", evt.ID.String(), "error", markErr.Error())
			}
			if p.metrics != nil && p.metrics.EventsFailedTotal != nil {
				p.metrics.EventsFailedTotal.WithLabelValues(p.workerType, evt.EventType, "unknown").Inc()
			}
			continue
		}

		// 2. Resolve Topic
		topic, err := p.topicResolver.ResolveTopic(evt.EventType)
		if err != nil {
			p.logger.Error("Failed to resolve Kafka topic, marking as FAILED",
				"worker_type", p.workerType,
				"event_id", evt.ID.String(),
				"tenant_code", evt.TenantCode,
				"event_type", evt.EventType,
				"error", err.Error(),
			)
			if markErr := p.repo.MarkFailed(ctx, evt.ID, err); markErr != nil {
				p.logger.Error("Failed to mark event as FAILED in database", "event_id", evt.ID.String(), "error", markErr.Error())
			}
			if p.metrics != nil && p.metrics.EventsFailedTotal != nil {
				p.metrics.EventsFailedTotal.WithLabelValues(p.workerType, evt.EventType, "unknown").Inc()
			}
			continue
		}

		// 3. Partition Key: deterministic aggregate ID
		partitionKey := evt.AggregateID.String()

		// 4. Publish to Kafka with in-memory retry loop (Option A)
		publishSuccess := false
		maxAttempts := p.cfg.Outbox.MaxRetries
		if maxAttempts <= 0 {
			maxAttempts = 1
		}

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			pubStart := time.Now()
			pubErr := p.publisher.Publish(ctx, topic, partitionKey, envelope)
			pubDuration := time.Since(pubStart).Seconds()

			if pubErr == nil {
				publishSuccess = true
				if p.metrics != nil {
					if p.metrics.PublishDurationSeconds != nil {
						p.metrics.PublishDurationSeconds.WithLabelValues(p.workerType, evt.EventType, topic).Observe(pubDuration)
					}
					if p.metrics.EventsPublishedTotal != nil {
						p.metrics.EventsPublishedTotal.WithLabelValues(p.workerType, evt.EventType, topic).Inc()
					}
				}

				p.logger.Info("outbox event published",
					"worker_type", p.workerType,
					"event_id", evt.ID.String(),
					"tenant_code", evt.TenantCode,
					"aggregate_type", evt.AggregateType,
					"aggregate_id", evt.AggregateID.String(),
					"event_type", evt.EventType,
					"topic", topic,
				)
				break
			}

			// Handle publish error
			if p.metrics != nil && p.metrics.KafkaPublishErrorsTotal != nil {
				p.metrics.KafkaPublishErrorsTotal.WithLabelValues(p.workerType, evt.EventType, topic).Inc()
			}

			isTransient := p.retryPolicy.IsTransient(pubErr)
			p.logger.Warn("Failed to publish outbox event to Kafka",
				"worker_type", p.workerType,
				"event_id", evt.ID.String(),
				"tenant_code", evt.TenantCode,
				"event_type", evt.EventType,
				"topic", topic,
				"attempt", attempt,
				"max_attempts", maxAttempts,
				"is_transient", isTransient,
				"error", pubErr.Error(),
			)

			if !isTransient || attempt == maxAttempts {
				// Permanent failure or retry exhausted
				p.logger.Error("failed to publish outbox event, marking as FAILED",
					"worker_type", p.workerType,
					"event_id", evt.ID.String(),
					"event_type", evt.EventType,
					"error", pubErr.Error(),
				)
				if markErr := p.repo.MarkFailed(ctx, evt.ID, pubErr); markErr != nil {
					p.logger.Error("Failed to update status to FAILED in database", "event_id", evt.ID.String(), "error", markErr.Error())
				}
				if p.metrics != nil && p.metrics.EventsFailedTotal != nil {
					p.metrics.EventsFailedTotal.WithLabelValues(p.workerType, evt.EventType, topic).Inc()
				}
				break
			}

			// Backoff before next attempt
			backoff := p.retryPolicy.Backoff(attempt)
			select {
			case <-ctx.Done():
				break
			case <-time.After(backoff):
			}
		}

		if publishSuccess {
			publishedIDs = append(publishedIDs, evt.ID)
		}
	}

	// 5. Batch update status to PUBLISHED in PostgreSQL
	if len(publishedIDs) > 0 {
		updateCtx, cancelUpdate := context.WithTimeout(context.Background(), 5*time.Second)
		err := p.repo.MarkPublished(updateCtx, publishedIDs, time.Now().UTC())
		cancelUpdate()
		if err != nil {
			p.logger.Error("Failed to mark batch as published in database",
				"worker_type", p.workerType,
				"count", len(publishedIDs),
				"error", err.Error(),
			)
			return 0, fmt.Errorf("failed to update published status: %w", err)
		}
	}

	duration := time.Since(start).Seconds()
	if p.metrics != nil && p.metrics.ProcessingDurationSeconds != nil {
		p.metrics.ProcessingDurationSeconds.WithLabelValues(p.workerType).Observe(duration)
	}

	return count, nil
}
