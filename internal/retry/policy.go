package retry

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"strings"
	"time"
)

// Policy defines retry settings and error classification.
type Policy struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	rnd        *rand.Rand
}

// NewPolicy creates a new Retry Policy.
func NewPolicy(maxRetries int, baseDelay time.Duration) *Policy {
	if maxRetries <= 0 {
		maxRetries = 5
	}
	if baseDelay <= 0 {
		baseDelay = 250 * time.Millisecond
	}
	return &Policy{
		MaxRetries: maxRetries,
		BaseDelay:  baseDelay,
		MaxDelay:   30 * time.Second,
		rnd:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// IsTransient evaluates whether an error is transient (retryable) or permanent.
func (p *Policy) IsTransient(err error) bool {
	if err == nil {
		return false
	}

	// Context canceled or deadline exceeded
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	// Network / connection errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	errMsg := strings.ToLower(err.Error())
	transientPhrases := []string{
		"connection refused",
		"connection reset",
		"timeout",
		"broken pipe",
		"i/o timeout",
		"leader not available",
		"not leader for partition",
		"network error",
		"temporary failure",
		"dial tcp",
		"eof",
	}

	for _, phrase := range transientPhrases {
		if strings.Contains(errMsg, phrase) {
			return true
		}
	}

	// Permanent errors: invalid json, bad payload, unroutable topic format
	permanentPhrases := []string{
		"invalid json payload",
		"unsupported event version",
		"invalid character",
		"unmarshal",
	}

	for _, phrase := range permanentPhrases {
		if strings.Contains(errMsg, phrase) {
			return false
		}
	}

	return true // default to transient for unknown broker/db errors
}

// Backoff computes exponential backoff with full jitter for attempt (1-indexed).
func (p *Policy) Backoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	// Exponential backoff: base * 2^(attempt-1)
	multiplier := 1 << uint(attempt-1)
	if multiplier > 64 { // prevent overflow
		multiplier = 64
	}

	temp := p.BaseDelay * time.Duration(multiplier)
	if temp > p.MaxDelay {
		temp = p.MaxDelay
	}

	// Apply jitter: random duration between 0 and temp
	if temp <= 0 {
		return p.BaseDelay
	}

	jitter := time.Duration(p.rnd.Int63n(int64(temp)))
	return p.BaseDelay/2 + jitter/2
}
