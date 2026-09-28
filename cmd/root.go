package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "hros-event-worker",
	Short: "HROS Outbox Kafka Relay Worker",
	Long:  `HROS Outbox Kafka Relay Worker is a generic standalone infrastructure worker that reads pending records from PostgreSQL Outbox tables and publishes them to Kafka across setting, access, and directory domains.`,
}

// Execute runs the root CLI command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(versionCmd)
}
