package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPolicy_IsTransient(t *testing.T) {
	p := NewPolicy(3, 100*time.Millisecond)

	assert.False(t, p.IsTransient(nil))
	assert.True(t, p.IsTransient(context.DeadlineExceeded))
	assert.True(t, p.IsTransient(context.Canceled))
	assert.True(t, p.IsTransient(errors.New("dial tcp 127.0.0.1:9092: connection refused")))
	assert.True(t, p.IsTransient(errors.New("i/o timeout")))
	assert.True(t, p.IsTransient(errors.New("not leader for partition")))

	// Permanent errors
	assert.False(t, p.IsTransient(errors.New("invalid json payload for event id: 123")))
	assert.False(t, p.IsTransient(errors.New("invalid character 'a' looking for beginning of value")))
}

func TestPolicy_Backoff(t *testing.T) {
	p := NewPolicy(5, 100*time.Millisecond)

	b1 := p.Backoff(1)
	assert.True(t, b1 >= 50*time.Millisecond)
	assert.True(t, b1 <= 150*time.Millisecond)

	b3 := p.Backoff(3)
	assert.True(t, b3 >= 50*time.Millisecond)
	assert.True(t, b3 <= 500*time.Millisecond)
}
