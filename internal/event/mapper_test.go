package event

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapToEnvelope_Success(t *testing.T) {
	eventID := uuid.New()
	createdAt := time.Now().UTC()

	env, err := MapToEnvelope(
		eventID,
		"tenant-alpha",
		"directory.employee.created",
		1,
		createdAt,
		[]byte(`{"name":"Alice","role":"Engineer"}`),
		"hros-directory-service",
	)
	require.NoError(t, err)
	require.NotNil(t, env)

	assert.Equal(t, eventID, env.EventID)
	assert.Equal(t, "directory.employee.created", env.EventType)
	assert.Equal(t, 1, env.EventVersion)
	assert.Equal(t, "tenant-alpha", env.TenantCode)
	assert.Equal(t, createdAt, env.OccurredAt)
	assert.Equal(t, "hros-directory-service", env.Producer)
	assert.JSONEq(t, `{"name":"Alice","role":"Engineer"}`, string(env.Payload))

	// Ensure JSON marshaling works properly
	data, err := json.Marshal(env)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"eventId":"`+eventID.String()+`"`)
	assert.Contains(t, string(data), `"tenantCode":"tenant-alpha"`)
}

func TestMapToEnvelope_InvalidPayload(t *testing.T) {
	_, err := MapToEnvelope(
		uuid.New(),
		"tenant-beta",
		"directory.employee.created",
		1,
		time.Now(),
		[]byte(`{invalid json`),
		"hros-directory-service",
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid json payload")
}

func TestMapToEnvelope_NilEventID(t *testing.T) {
	_, err := MapToEnvelope(
		uuid.Nil,
		"tenant-beta",
		"directory.employee.created",
		1,
		time.Now(),
		[]byte(`{}`),
		"producer",
	)
	assert.Error(t, err)
}
