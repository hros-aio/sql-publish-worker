package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"hros-event-worker/internal/config"
	"hros-event-worker/internal/event"
)

type SaramaPublisher struct {
	client   sarama.Client
	producer sarama.SyncProducer
}

// NewSaramaPublisher initializes a Sarama sync producer with idempotent settings and durable acks.
func NewSaramaPublisher(cfg config.KafkaConfig) (*SaramaPublisher, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = cfg.ClientID
	saramaConfig.Version = sarama.V3_3_0_0 // Standard modern Kafka broker version

	// Durable acknowledgment
	switch strings.ToLower(cfg.RequiredAcks) {
	case "0", "none", "no_response":
		saramaConfig.Producer.RequiredAcks = sarama.NoResponse
	case "1", "local", "wait_for_local":
		saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	default:
		saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	}

	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	// Idempotent producer settings
	if saramaConfig.Producer.RequiredAcks == sarama.WaitForAll {
		saramaConfig.Producer.Idempotent = true
		saramaConfig.Net.MaxOpenRequests = 1
	}

	if cfg.RetryMax > 0 {
		saramaConfig.Producer.Retry.Max = cfg.RetryMax
	} else {
		saramaConfig.Producer.Retry.Max = 5
	}

	if cfg.RetryBackoff > 0 {
		saramaConfig.Producer.Retry.Backoff = cfg.RetryBackoff
	} else {
		saramaConfig.Producer.Retry.Backoff = 250 * time.Millisecond
	}

	// Compression codec
	switch strings.ToLower(cfg.Compression) {
	case "snappy":
		saramaConfig.Producer.Compression = sarama.CompressionSnappy
	case "gzip":
		saramaConfig.Producer.Compression = sarama.CompressionGZIP
	case "lz4":
		saramaConfig.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		saramaConfig.Producer.Compression = sarama.CompressionZSTD
	default:
		saramaConfig.Producer.Compression = sarama.CompressionNone
	}

	client, err := sarama.NewClient(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create sarama client: %w", err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to create sarama sync producer: %w", err)
	}

	return &SaramaPublisher{
		client:   client,
		producer: producer,
	}, nil
}

// NewSaramaPublisherWithProducer creates a publisher with an existing producer (for testing).
func NewSaramaPublisherWithProducer(producer sarama.SyncProducer, client sarama.Client) *SaramaPublisher {
	return &SaramaPublisher{
		client:   client,
		producer: producer,
	}
}

// Publish serializes and sends the event envelope to the target Kafka topic with stable partition key.
func (p *SaramaPublisher) Publish(ctx context.Context, topic string, partitionKey string, envelope *event.Envelope) error {
	if envelope == nil {
		return fmt.Errorf("envelope cannot be nil")
	}

	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize event envelope: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(partitionKey),
		Value: sarama.ByteEncoder(payloadBytes),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("event-id"),
				Value: []byte(envelope.EventID.String()),
			},
			{
				Key:   []byte("event-type"),
				Value: []byte(envelope.EventType),
			},
			{
				Key:   []byte("tenant-code"),
				Value: []byte(envelope.TenantCode),
			},
			{
				Key:   []byte("content-type"),
				Value: []byte("application/json"),
			},
		},
		Timestamp: envelope.OccurredAt,
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("kafka producer failed to send message: %w", err)
	}

	return nil
}

func (p *SaramaPublisher) Ping(ctx context.Context) error {
	if p.client == nil || p.client.Closed() {
		return fmt.Errorf("sarama client is closed or uninitialized")
	}
	brokers := p.client.Brokers()
	if len(brokers) == 0 {
		return fmt.Errorf("no active kafka brokers found")
	}
	return nil
}

func (p *SaramaPublisher) Close() error {
	var errs []string
	if p.producer != nil {
		if err := p.producer.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to close producer: %v", err))
		}
	}
	if p.client != nil && !p.client.Closed() {
		if err := p.client.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to close client: %v", err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}
