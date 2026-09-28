package config

import "time"

// RootConfig contains service-specific configurations for all three supported domains.
type RootConfig struct {
	Setting   DomainConfig `yaml:"setting"`
	Access    DomainConfig `yaml:"access"`
	Directory DomainConfig `yaml:"directory"`
}

// DomainConfig encapsulates all operational settings for a single domain outbox worker.
type DomainConfig struct {
	Database DatabaseConfig `yaml:"database"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Outbox   OutboxConfig   `yaml:"outbox"`
	Worker   WorkerConfig   `yaml:"worker"`
	HTTP     HTTPConfig     `yaml:"http"`
	LogLevel string         `yaml:"log_level"`
}

type DatabaseConfig struct {
	Host            string        `yaml:"host"`
	Port            int           `yaml:"port"`
	Name            string        `yaml:"name"`
	User            string        `yaml:"user"`
	Password        string        `yaml:"password"`
	SSLMode         string        `yaml:"ssl_mode"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

type KafkaConfig struct {
	Brokers      []string      `yaml:"brokers"`
	ClientID     string        `yaml:"client_id"`
	TopicPrefix  string        `yaml:"topic_prefix"`
	RequiredAcks string        `yaml:"required_acks"` // "all", "1", "0"
	RetryMax     int           `yaml:"retry_max"`
	RetryBackoff time.Duration `yaml:"retry_backoff"`
	Compression  string        `yaml:"compression"` // "none", "gzip", "snappy", "lz4", "zstd"
	ProducerName string        `yaml:"producer_name"`
}

type OutboxConfig struct {
	BatchSize         int           `yaml:"batch_size"`
	PollInterval      time.Duration `yaml:"poll_interval"`
	MaxRetries        int           `yaml:"max_retries"`
	RetryBackoff      time.Duration `yaml:"retry_backoff"`
	ClaimTimeout      time.Duration `yaml:"claim_timeout"`
	ProcessingTimeout time.Duration `yaml:"processing_timeout"`
}

type WorkerConfig struct {
	Concurrency     int           `yaml:"concurrency"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type HTTPConfig struct {
	Port int `yaml:"port"`
}

// DefaultDomainConfig returns sensible defaults for a domain worker.
func DefaultDomainConfig(workerType string) DomainConfig {
	return DomainConfig{
		Database: DatabaseConfig{
			Host:            "localhost",
			Port:            5432,
			Name:            "hros_" + workerType,
			User:            "postgres",
			Password:        "",
			SSLMode:         "disable",
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: 5 * time.Minute,
		},
		Kafka: KafkaConfig{
			Brokers:      []string{"localhost:9092"},
			ClientID:     "hros-" + workerType + "-event-worker",
			TopicPrefix:  "",
			RequiredAcks: "all",
			RetryMax:     5,
			RetryBackoff: 250 * time.Millisecond,
			Compression:  "snappy",
			ProducerName: "hros-" + workerType + "-service",
		},
		Outbox: OutboxConfig{
			BatchSize:         100,
			PollInterval:      1 * time.Second,
			MaxRetries:        5,
			RetryBackoff:      500 * time.Millisecond,
			ClaimTimeout:      5 * time.Second,
			ProcessingTimeout: 30 * time.Second,
		},
		Worker: WorkerConfig{
			Concurrency:     2,
			ShutdownTimeout: 15 * time.Second,
		},
		HTTP: HTTPConfig{
			Port: 8080,
		},
		LogLevel: "info",
	}
}
