package applynotifications

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type sqsPublisher struct {
	client   *sqs.Client
	queueURL string
}

// NewSQS uses the standard AWS configuration and credential chain. Sending is
// one SDK attempt: queue failures are dropped, not retried by the SDK.
func NewSQS(ctx context.Context, queueURL string) (Publisher, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config for apply notifications: %w", err)
	}
	return &sqsPublisher{
		client: sqs.NewFromConfig(cfg, func(opts *sqs.Options) {
			opts.RetryMaxAttempts = 1
		}),
		queueURL: queueURL,
	}, nil
}

func (p *sqsPublisher) Publish(ctx context.Context, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode completion notification for apply %s: %w", event.ApplyIdentifier, err)
	}
	_, err = p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(p.queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		return fmt.Errorf("send completion notification for apply %s: %w", event.ApplyIdentifier, err)
	}
	return nil
}
