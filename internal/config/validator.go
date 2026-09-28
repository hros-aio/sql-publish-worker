package config

import (
	"errors"
	"fmt"
)

// ValidateConfig validates all domain configuration fields.
func ValidateConfig(cfg *DomainConfig) error {
	if cfg == nil {
		return errors.New("configuration is nil")
	}

	// Validate Database
	if cfg.Database.Host == "" {
		return errors.New("database host is required")
	}
	if cfg.Database.Port <= 0 || cfg.Database.Port > 65535 {
		return fmt.Errorf("invalid database port: %d", cfg.Database.Port)
	}
	if cfg.Database.Name == "" {
		return errors.New("database name is required")
	}
	if cfg.Database.User == "" {
		return errors.New("database user is required")
	}

	// Validate Kafka
	if len(cfg.Kafka.Brokers) == 0 {
		return errors.New("kafka brokers list cannot be empty")
	}
	for _, b := range cfg.Kafka.Brokers {
		if b == "" {
			return errors.New("kafka broker address cannot be empty")
		}
	}
	if cfg.Kafka.ClientID == "" {
		return errors.New("kafka client_id is required")
	}

	// Validate Outbox
	if cfg.Outbox.BatchSize <= 0 {
		return fmt.Errorf("outbox batch_size must be greater than 0, got %d", cfg.Outbox.BatchSize)
	}
	if cfg.Outbox.PollInterval <= 0 {
		return fmt.Errorf("outbox poll_interval must be greater than 0, got %s", cfg.Outbox.PollInterval)
	}
	if cfg.Outbox.MaxRetries < 0 {
		return fmt.Errorf("outbox max_retries must be non-negative, got %d", cfg.Outbox.MaxRetries)
	}

	// Validate Worker
	if cfg.Worker.Concurrency <= 0 {
		return fmt.Errorf("worker concurrency must be greater than 0, got %d", cfg.Worker.Concurrency)
	}
	if cfg.Worker.ShutdownTimeout <= 0 {
		return fmt.Errorf("worker shutdown_timeout must be greater than 0, got %s", cfg.Worker.ShutdownTimeout)
	}

	return nil
}
