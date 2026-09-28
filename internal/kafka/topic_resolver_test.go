package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultTopicResolver(t *testing.T) {
	resolverWithoutPrefix := NewTopicResolver("")
	resolverWithPrefix := NewTopicResolver("hros")

	tests := []struct {
		name          string
		resolver      *DefaultTopicResolver
		eventType     string
		expectedTopic string
		expectError   bool
	}{
		{
			name:          "Directory employee created without prefix",
			resolver:      resolverWithoutPrefix,
			eventType:     "directory.employee.created",
			expectedTopic: "directory.employee",
			expectError:   false,
		},
		{
			name:          "Setting company updated without prefix",
			resolver:      resolverWithoutPrefix,
			eventType:     "setting.company.updated",
			expectedTopic: "setting.company",
			expectError:   false,
		},
		{
			name:          "Access role assigned without prefix",
			resolver:      resolverWithoutPrefix,
			eventType:     "access.role.assigned",
			expectedTopic: "access.role",
			expectError:   false,
		},
		{
			name:          "Directory employee created with prefix",
			resolver:      resolverWithPrefix,
			eventType:     "directory.employee.created",
			expectedTopic: "hros.directory.employee",
			expectError:   false,
		},
		{
			name:          "Single segment event type",
			resolver:      resolverWithoutPrefix,
			eventType:     "notifications",
			expectedTopic: "notifications",
			expectError:   false,
		},
		{
			name:          "Empty event type errors",
			resolver:      resolverWithoutPrefix,
			eventType:     "",
			expectedTopic: "",
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, err := tt.resolver.ResolveTopic(tt.eventType)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedTopic, topic)
			}
		})
	}
}
