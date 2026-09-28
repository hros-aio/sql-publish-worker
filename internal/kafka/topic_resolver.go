package kafka

import (
	"fmt"
	"strings"
)

// TopicResolver resolves a Kafka topic name from an event type and optional prefix.
type TopicResolver interface {
	ResolveTopic(eventType string) (string, error)
}

type DefaultTopicResolver struct {
	prefix string
}

// NewTopicResolver creates a new default topic resolver.
func NewTopicResolver(prefix string) *DefaultTopicResolver {
	return &DefaultTopicResolver{
		prefix: strings.TrimSuffix(prefix, "."),
	}
}

// ResolveTopic converts an event type like "directory.employee.created" to topic "directory.employee".
// If prefix is set to "hros", it produces "hros.directory.employee".
func (r *DefaultTopicResolver) ResolveTopic(eventType string) (string, error) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return "", fmt.Errorf("event_type cannot be empty")
	}

	parts := strings.Split(eventType, ".")
	var baseTopic string
	if len(parts) >= 2 {
		baseTopic = parts[0] + "." + parts[1]
	} else {
		baseTopic = parts[0]
	}

	if r.prefix != "" {
		if strings.HasPrefix(baseTopic, r.prefix+".") {
			return baseTopic, nil
		}
		return r.prefix + "." + baseTopic, nil
	}

	return baseTopic, nil
}
