package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"hros-event-worker/internal/config"
	"hros-event-worker/internal/health"
	"hros-event-worker/internal/kafka"
	"hros-event-worker/internal/observability"
	"hros-event-worker/internal/outbox"
	"hros-event-worker/internal/worker"
)

var (
	workerType string
	configPath string
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the outbox event relay worker for a specific service domain",
	RunE: func(cmd *cobra.Command, args []string) error {
		if workerType == "" {
			return fmt.Errorf("worker type is required (--type). supported types: setting, access, directory")
		}

		if !config.IsValidWorkerType(workerType) {
			return fmt.Errorf("invalid worker type: %s\nsupported types: setting, access, directory", workerType)
		}

		// 1. Load and validate configuration
		cfg, err := config.LoadConfig(workerType, configPath)
		if err != nil {
			return fmt.Errorf("failed to load configuration: %w", err)
		}

		// 2. Initialize observability
		logger := observability.InitLogger(cfg.LogLevel)
		metrics := observability.InitMetrics(nil)

		logger.Info("Initializing worker components", "worker_type", workerType)

		// 3. Initialize PostgreSQL Repository
		repo, err := outbox.NewPostgresRepository(cfg.Database)
		if err != nil {
			logger.Error("Failed to initialize PostgreSQL repository", "error", err.Error())
			return fmt.Errorf("failed to connect to database: %w", err)
		}

		// 4. Initialize Kafka Publisher
		publisher, err := kafka.NewSaramaPublisher(cfg.Kafka)
		if err != nil {
			_ = repo.Close()
			logger.Error("Failed to initialize Kafka publisher", "error", err.Error())
			return fmt.Errorf("failed to connect to kafka: %w", err)
		}

		// 5. Initialize Topic Resolver
		topicResolver := kafka.NewTopicResolver(cfg.Kafka.TopicPrefix)

		// 6. Initialize Outbox Processor
		processor := outbox.NewProcessor(
			workerType,
			*cfg,
			repo,
			publisher,
			topicResolver,
			metrics,
			logger,
		)

		// 7. Initialize Health and Metrics HTTP Server
		healthServer := health.NewServer(cfg.HTTP.Port, repo, publisher)

		// 8. Initialize and start Runner
		runner := worker.NewRunner(
			workerType,
			cfg,
			repo,
			publisher,
			processor,
			healthServer,
			logger,
		)

		return runner.Run(context.Background())
	},
}

func init() {
	runCmd.Flags().StringVarP(&workerType, "type", "t", "", "Service worker domain type (setting, access, directory)")
	runCmd.Flags().StringVarP(&configPath, "config", "c", "./config.yaml", "Path to YAML configuration file")
}
