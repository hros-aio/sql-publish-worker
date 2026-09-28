package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsValidWorkerType(t *testing.T) {
	assert.True(t, IsValidWorkerType("setting"))
	assert.True(t, IsValidWorkerType("access"))
	assert.True(t, IsValidWorkerType("directory"))
	assert.False(t, IsValidWorkerType("foo"))
	assert.False(t, IsValidWorkerType(""))
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := LoadConfig("directory", "")
	require.NoError(t, err)
	assert.Equal(t, "localhost", cfg.Database.Host)
	assert.Equal(t, 5432, cfg.Database.Port)
	assert.Equal(t, "hros_directory", cfg.Database.Name)
	assert.Equal(t, "hros-directory-event-worker", cfg.Kafka.ClientID)
	assert.Equal(t, 100, cfg.Outbox.BatchSize)
	assert.Equal(t, 1*time.Second, cfg.Outbox.PollInterval)
}

func TestLoadConfig_InvalidType(t *testing.T) {
	_, err := LoadConfig("invalid_type", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid worker type: invalid_type")
}

func TestLoadConfig_YAMLAndEnvOverrides(t *testing.T) {
	yamlContent := `
setting:
  database:
    host: yaml-db-host
    port: 5433
    name: yaml_setting_db
    user: yaml_user
  kafka:
    brokers:
      - yaml-broker:9092
    client_id: yaml-client-id
  outbox:
    batch_size: 200
`
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write([]byte(yamlContent))
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	// Set Environment Variable Override
	t.Setenv("HROS_SETTING_DATABASE_HOST", "env-db-host")
	t.Setenv("HROS_SETTING_OUTBOX_BATCH_SIZE", "500")

	cfg, err := LoadConfig("setting", tmpFile.Name())
	require.NoError(t, err)

	// ENV overrides YAML
	assert.Equal(t, "env-db-host", cfg.Database.Host)
	assert.Equal(t, 500, cfg.Outbox.BatchSize)

	// YAML overrides defaults
	assert.Equal(t, 5433, cfg.Database.Port)
	assert.Equal(t, "yaml_setting_db", cfg.Database.Name)
	assert.Equal(t, "yaml_user", cfg.Database.User)
	assert.Equal(t, []string{"yaml-broker:9092"}, cfg.Kafka.Brokers)
	assert.Equal(t, "yaml-client-id", cfg.Kafka.ClientID)
}

func TestValidateConfig(t *testing.T) {
	cfg := DefaultDomainConfig("access")
	assert.NoError(t, ValidateConfig(&cfg))

	invalidCfg := cfg
	invalidCfg.Database.Host = ""
	assert.Error(t, ValidateConfig(&invalidCfg))

	invalidCfg = cfg
	invalidCfg.Kafka.Brokers = []string{}
	assert.Error(t, ValidateConfig(&invalidCfg))

	invalidCfg = cfg
	invalidCfg.Outbox.BatchSize = 0
	assert.Error(t, ValidateConfig(&invalidCfg))
}

func TestLoadConfig_InvalidEnvOverrides(t *testing.T) {
	t.Setenv("HROS_ACCESS_DATABASE_PORT", "invalid_port")
	_, err := LoadConfig("access", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid HROS_ACCESS_DATABASE_PORT")

	t.Setenv("HROS_ACCESS_DATABASE_PORT", "5432")
	t.Setenv("HROS_ACCESS_OUTBOX_POLL_INTERVAL", "not_a_duration")
	_, err = LoadConfig("access", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid HROS_ACCESS_OUTBOX_POLL_INTERVAL")
}
