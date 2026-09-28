package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	Version   = "1.0.0"
	BuildDate = "2026-09-28"
	GitCommit = "development"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version and build information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("hros-event-worker version %s (built %s, commit %s)\n", Version, BuildDate, GitCommit)
	},
}
