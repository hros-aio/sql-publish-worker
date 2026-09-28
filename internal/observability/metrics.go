package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	PollTotal                *prometheus.CounterVec
	PollErrorsTotal          *prometheus.CounterVec
	EventsClaimedTotal       *prometheus.CounterVec
	EventsPublishedTotal     *prometheus.CounterVec
	EventsFailedTotal        *prometheus.CounterVec
	PublishDurationSeconds   *prometheus.HistogramVec
	ProcessingDurationSeconds *prometheus.HistogramVec
	PendingEvents            *prometheus.GaugeVec
	BatchSize                *prometheus.GaugeVec
	KafkaPublishErrorsTotal  *prometheus.CounterVec
}

var DefaultMetrics *Metrics

func InitMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	factory := promauto.With(reg)

	m := &Metrics{
		PollTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "outbox_poll_total",
				Help: "Total number of outbox polling attempts",
			},
			[]string{"worker_type"},
		),
		PollErrorsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "outbox_poll_errors_total",
				Help: "Total number of outbox polling errors",
			},
			[]string{"worker_type"},
		),
		EventsClaimedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "outbox_events_claimed_total",
				Help: "Total number of outbox events claimed from database",
			},
			[]string{"worker_type"},
		),
		EventsPublishedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "outbox_events_published_total",
				Help: "Total number of outbox events successfully published to Kafka",
			},
			[]string{"worker_type", "event_type", "topic"},
		),
		EventsFailedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "outbox_events_failed_total",
				Help: "Total number of outbox events permanently marked as failed",
			},
			[]string{"worker_type", "event_type", "topic"},
		),
		PublishDurationSeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "outbox_publish_duration_seconds",
				Help:    "Duration in seconds spent publishing an event to Kafka",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"worker_type", "event_type", "topic"},
		),
		ProcessingDurationSeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "outbox_processing_duration_seconds",
				Help:    "Duration in seconds spent processing a batch of outbox events",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"worker_type"},
		),
		PendingEvents: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "outbox_pending_events",
				Help: "Current estimated pending outbox events backlog",
			},
			[]string{"worker_type"},
		),
		BatchSize: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "outbox_batch_size",
				Help: "Configured or observed outbox batch size",
			},
			[]string{"worker_type"},
		),
		KafkaPublishErrorsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_publish_errors_total",
				Help: "Total number of Kafka publish errors",
			},
			[]string{"worker_type", "event_type", "topic"},
		),
	}

	DefaultMetrics = m
	return m
}
