package worker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"hros-event-worker/internal/config"
	"hros-event-worker/internal/health"
	"hros-event-worker/internal/kafka"
	"hros-event-worker/internal/outbox"
)

type Runner struct {
	workerType   string
	cfg          *config.DomainConfig
	repo         outbox.Repository
	publisher    kafka.Publisher
	processor    *outbox.Processor
	healthServer *health.Server
	logger       *slog.Logger
}

func NewRunner(
	workerType string,
	cfg *config.DomainConfig,
	repo outbox.Repository,
	publisher kafka.Publisher,
	processor *outbox.Processor,
	healthServer *health.Server,
	logger *slog.Logger,
) *Runner {
	if logger == nil {
		logger = slog.Default()
	}

	return &Runner{
		workerType:   workerType,
		cfg:          cfg,
		repo:         repo,
		publisher:    publisher,
		processor:    processor,
		healthServer: healthServer,
		logger:       logger,
	}
}

// Run starts the health server, starts worker polling goroutines, and blocks until shutdown signal.
func (r *Runner) Run(ctx context.Context) error {
	r.logger.Info("Starting HROS Outbox Event Worker",
		"worker_type", r.workerType,
		"batch_size", r.cfg.Outbox.BatchSize,
		"poll_interval", r.cfg.Outbox.PollInterval.String(),
		"concurrency", r.cfg.Worker.Concurrency,
	)

	// Start Health & Metrics HTTP server if configured
	if r.healthServer != nil && r.cfg.HTTP.Port > 0 {
		go func() {
			if err := r.healthServer.Start(); err != nil {
				r.logger.Error("Health server stopped with error", "error", err.Error())
			}
		}()
		r.logger.Info("HTTP health & metrics server started", "port", r.cfg.HTTP.Port)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Capture OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	var wg sync.WaitGroup
	concurrency := r.cfg.Worker.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r.workerLoop(ctx, workerID)
		}(i + 1)
	}

	// Wait for termination signal or context cancellation
	select {
	case sig := <-sigChan:
		r.logger.Info("Received termination signal, initiating graceful shutdown", "signal", sig.String())
	case <-ctx.Done():
		r.logger.Info("Context cancelled, stopping worker")
	}

	// Stop polling workers
	cancel()

	// Graceful shutdown with timeout
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), r.cfg.Worker.ShutdownTimeout)
	defer cancelShutdown()

	shutdownComplete := make(chan struct{})
	go func() {
		wg.Wait()

		if r.healthServer != nil {
			_ = r.healthServer.Shutdown(shutdownCtx)
		}

		if r.publisher != nil {
			_ = r.publisher.Close()
		}

		if r.repo != nil {
			_ = r.repo.Close()
		}

		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		r.logger.Info("Worker graceful shutdown completed cleanly", "worker_type", r.workerType)
		return nil
	case <-shutdownCtx.Done():
		r.logger.Warn("Shutdown timed out before all resources closed cleanly", "worker_type", r.workerType)
		return errors.New("shutdown timed out")
	}
}

func (r *Runner) workerLoop(ctx context.Context, workerID int) {
	r.logger.Debug("Worker loop started", "worker_type", r.workerType, "worker_id", workerID)

	dbErrorCount := 0

	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("Worker loop exiting", "worker_type", r.workerType, "worker_id", workerID)
			return
		default:
		}

		count, err := r.processor.ProcessBatch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			dbErrorCount++
			backoff := time.Duration(dbErrorCount) * 500 * time.Millisecond
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}

			r.logger.Error("Database or batch processing error, backing off",
				"worker_type", r.workerType,
				"worker_id", workerID,
				"backoff", backoff.String(),
				"error", err.Error(),
			)

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}

		dbErrorCount = 0

		if count == 0 {
			// No events found, sleep for poll interval
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.cfg.Outbox.PollInterval):
			}
		}
		// If count > 0, immediately check for next batch without sleeping
	}
}
