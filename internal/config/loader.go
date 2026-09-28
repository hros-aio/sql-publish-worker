package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// SupportedWorkerTypes lists valid domain types.
var SupportedWorkerTypes = []string{"setting", "access", "directory"}

// IsValidWorkerType checks if a type string is one of the supported domain types.
func IsValidWorkerType(t string) bool {
	for _, supported := range SupportedWorkerTypes {
		if t == supported {
			return true
		}
	}
	return false
}

// LoadConfig loads and merges defaults, YAML file, and environment variables for the specified worker type.
func LoadConfig(workerType, configPath string) (*DomainConfig, error) {
	if !IsValidWorkerType(workerType) {
		return nil, fmt.Errorf("invalid worker type: %s. supported types: %s",
			workerType, strings.Join(SupportedWorkerTypes, ", "))
	}

	cfg := DefaultDomainConfig(workerType)

	// Read YAML if path exists
	if configPath != "" {
		if _, err := os.Stat(configPath); err == nil {
			data, err := os.ReadFile(configPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
			}

			var root RootConfig
			if err := yaml.Unmarshal(data, &root); err != nil {
				return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
			}

			var selected DomainConfig
			switch workerType {
			case "setting":
				selected = root.Setting
			case "access":
				selected = root.Access
			case "directory":
				selected = root.Directory
			}

			mergeDomainConfig(&cfg, &selected)
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("error accessing config file %s: %w", configPath, err)
		}
	}

	// Apply Environment Variables
	if err := applyEnvOverrides(workerType, &cfg); err != nil {
		return nil, fmt.Errorf("invalid environment variable override: %w", err)
	}

	// Validate config
	if err := ValidateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return &cfg, nil
}

func mergeDomainConfig(dst, src *DomainConfig) {
	// Database
	if src.Database.Host != "" {
		dst.Database.Host = src.Database.Host
	}
	if src.Database.Port != 0 {
		dst.Database.Port = src.Database.Port
	}
	if src.Database.Name != "" {
		dst.Database.Name = src.Database.Name
	}
	if src.Database.User != "" {
		dst.Database.User = src.Database.User
	}
	if src.Database.Password != "" {
		dst.Database.Password = src.Database.Password
	}
	if src.Database.SSLMode != "" {
		dst.Database.SSLMode = src.Database.SSLMode
	}
	if src.Database.MaxOpenConns != 0 {
		dst.Database.MaxOpenConns = src.Database.MaxOpenConns
	}
	if src.Database.MaxIdleConns != 0 {
		dst.Database.MaxIdleConns = src.Database.MaxIdleConns
	}
	if src.Database.ConnMaxLifetime != 0 {
		dst.Database.ConnMaxLifetime = src.Database.ConnMaxLifetime
	}

	// Kafka
	if len(src.Kafka.Brokers) > 0 {
		dst.Kafka.Brokers = src.Kafka.Brokers
	}
	if src.Kafka.ClientID != "" {
		dst.Kafka.ClientID = src.Kafka.ClientID
	}
	if src.Kafka.TopicPrefix != "" {
		dst.Kafka.TopicPrefix = src.Kafka.TopicPrefix
	}
	if src.Kafka.RequiredAcks != "" {
		dst.Kafka.RequiredAcks = src.Kafka.RequiredAcks
	}
	if src.Kafka.RetryMax != 0 {
		dst.Kafka.RetryMax = src.Kafka.RetryMax
	}
	if src.Kafka.RetryBackoff != 0 {
		dst.Kafka.RetryBackoff = src.Kafka.RetryBackoff
	}
	if src.Kafka.Compression != "" {
		dst.Kafka.Compression = src.Kafka.Compression
	}
	if src.Kafka.ProducerName != "" {
		dst.Kafka.ProducerName = src.Kafka.ProducerName
	}

	// Outbox
	if src.Outbox.BatchSize != 0 {
		dst.Outbox.BatchSize = src.Outbox.BatchSize
	}
	if src.Outbox.PollInterval != 0 {
		dst.Outbox.PollInterval = src.Outbox.PollInterval
	}
	if src.Outbox.MaxRetries != 0 {
		dst.Outbox.MaxRetries = src.Outbox.MaxRetries
	}
	if src.Outbox.RetryBackoff != 0 {
		dst.Outbox.RetryBackoff = src.Outbox.RetryBackoff
	}
	if src.Outbox.ClaimTimeout != 0 {
		dst.Outbox.ClaimTimeout = src.Outbox.ClaimTimeout
	}
	if src.Outbox.ProcessingTimeout != 0 {
		dst.Outbox.ProcessingTimeout = src.Outbox.ProcessingTimeout
	}

	// Worker
	if src.Worker.Concurrency != 0 {
		dst.Worker.Concurrency = src.Worker.Concurrency
	}
	if src.Worker.ShutdownTimeout != 0 {
		dst.Worker.ShutdownTimeout = src.Worker.ShutdownTimeout
	}

	// HTTP & Log
	if src.HTTP.Port != 0 {
		dst.HTTP.Port = src.HTTP.Port
	}
	if src.LogLevel != "" {
		dst.LogLevel = src.LogLevel
	}
}

func applyEnvOverrides(workerType string, cfg *DomainConfig) error {
	prefix := "HROS_" + strings.ToUpper(workerType) + "_"

	// Database
	if v := os.Getenv(prefix + "DATABASE_HOST"); v != "" {
		cfg.Database.Host = v
	}
	if v := os.Getenv(prefix + "DATABASE_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid %sDATABASE_PORT: %w", prefix, err)
		}
		cfg.Database.Port = p
	}
	if v := os.Getenv(prefix + "DATABASE_NAME"); v != "" {
		cfg.Database.Name = v
	}
	if v := os.Getenv(prefix + "DATABASE_USER"); v != "" {
		cfg.Database.User = v
	}
	if v := os.Getenv(prefix + "DATABASE_PASSWORD"); v != "" {
		cfg.Database.Password = v
	}
	if v := os.Getenv(prefix + "DATABASE_SSL_MODE"); v != "" {
		cfg.Database.SSLMode = v
	}

	// Kafka
	if v := os.Getenv(prefix + "KAFKA_BROKERS"); v != "" {
		cfg.Kafka.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv(prefix + "KAFKA_CLIENT_ID"); v != "" {
		cfg.Kafka.ClientID = v
	}
	if v := os.Getenv(prefix + "KAFKA_TOPIC_PREFIX"); v != "" {
		cfg.Kafka.TopicPrefix = v
	}
	if v := os.Getenv(prefix + "KAFKA_PRODUCER_NAME"); v != "" {
		cfg.Kafka.ProducerName = v
	}

	// Outbox
	if v := os.Getenv(prefix + "OUTBOX_BATCH_SIZE"); v != "" {
		bs, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid %sOUTBOX_BATCH_SIZE: %w", prefix, err)
		}
		cfg.Outbox.BatchSize = bs
	}
	if v := os.Getenv(prefix + "OUTBOX_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid %sOUTBOX_POLL_INTERVAL: %w", prefix, err)
		}
		cfg.Outbox.PollInterval = d
	}
	if v := os.Getenv(prefix + "OUTBOX_MAX_RETRIES"); v != "" {
		mr, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid %sOUTBOX_MAX_RETRIES: %w", prefix, err)
		}
		cfg.Outbox.MaxRetries = mr
	}

	// HTTP & Log
	if v := os.Getenv(prefix + "HTTP_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid %sHTTP_PORT: %w", prefix, err)
		}
		cfg.HTTP.Port = p
	}
	if v := os.Getenv(prefix + "LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}

	return nil
}
