package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	for _, tt := range []struct {
		name   string
		config Config
		valid  bool
	}{
		{name: "disabled", valid: true},
		{name: "sqs", config: Config{Publisher: "sqs", QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/completed"}, valid: true},
		{name: "unknown", config: Config{Publisher: "kafka"}},
		{name: "missing queue", config: Config{Publisher: "sqs"}},
		{name: "transport without publisher", config: Config{Region: "us-east-1"}},
		{name: "FIFO", config: Config{Publisher: "sqs", QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/completed.fifo"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.valid {
				require.NoError(t, tt.config.Validate())
			} else {
				require.Error(t, tt.config.Validate())
			}
		})
	}
	publisher, err := NewPublisher(t.Context(), Config{})
	require.NoError(t, err)
	assert.Nil(t, publisher)
}

type fakeSQS struct {
	input *sqs.SendMessageInput
	err   error
}

func (f *fakeSQS) SendMessage(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.input = input
	return &sqs.SendMessageOutput{}, f.err
}

func TestSQSPublisher(t *testing.T) {
	client := &fakeSQS{}
	publisher := &sqsPublisher{client: client, queueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/completed"}
	payload := json.RawMessage(`{"id":"apply_1","database":"orders","database_type":"mysql","environment":"production","completed_at":"2026-11-20T00:00:00Z"}`)
	require.NoError(t, publisher.Publish(t.Context(), payload))
	assert.Equal(t, publisher.queueURL, aws.ToString(client.input.QueueUrl))
	assert.JSONEq(t, string(payload), aws.ToString(client.input.MessageBody))
	assert.Nil(t, client.input.MessageGroupId)
	client.err = errors.New("SQS unavailable")
	require.ErrorContains(t, publisher.Publish(t.Context(), payload), "SQS unavailable")
}
