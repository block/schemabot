package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sqsStub struct {
	input *sqs.SendMessageInput
	err   error
}

func (s *sqsStub) SendMessage(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	s.input = input
	return &sqs.SendMessageOutput{}, s.err
}

func TestSQSPublisherPayloadAndErrors(t *testing.T) {
	for _, queue := range []string{"https://sqs.us-east-1.amazonaws.com/123/completions", "https://sqs.us-east-1.amazonaws.com/123/completions.fifo"} {
		t.Run(queue, func(t *testing.T) {
			client := &sqsStub{}
			publisher := &sqsPublisher{client: client, queueURL: queue}
			event := Event{ApplyIdentifier: "apply-stable", Database: "orders", DatabaseType: "postgres", Environment: "staging", CompletedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			require.NoError(t, publisher.Publish(t.Context(), event))
			assert.Equal(t, queue, aws.ToString(client.input.QueueUrl))
			if strings.HasSuffix(queue, ".fifo") {
				assert.Equal(t, "apply-completions", aws.ToString(client.input.MessageGroupId))
				assert.Equal(t, event.ApplyIdentifier, aws.ToString(client.input.MessageDeduplicationId))
			} else {
				assert.Nil(t, client.input.MessageGroupId)
				assert.Nil(t, client.input.MessageDeduplicationId)
			}
			var payload map[string]any
			require.NoError(t, json.Unmarshal([]byte(aws.ToString(client.input.MessageBody)), &payload))
			assert.Len(t, payload, 5)
			assert.Equal(t, "apply-stable", payload["apply_identifier"])
			assert.Equal(t, "2026-01-01T00:00:00Z", payload["completed_at"])
			event.Repository = "example/schema"
			event.PullRequest = 42
			require.NoError(t, publisher.Publish(t.Context(), event))
			require.NoError(t, json.Unmarshal([]byte(aws.ToString(client.input.MessageBody)), &payload))
			assert.Len(t, payload, 7)
			assert.Equal(t, float64(42), payload["pull_request"])
			client.err = errors.New("SQS unavailable")
			require.ErrorContains(t, publisher.Publish(t.Context(), event), "SQS unavailable")
		})
	}
}
