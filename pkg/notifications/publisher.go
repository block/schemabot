// Package notifications delivers durable apply completion notifications.
package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Config selects the completion notification transport. An empty publisher
// disables delivery, but completion intent is still retained in storage.
type Config struct {
	Publisher string `yaml:"publisher"`
	QueueURL  string `yaml:"queue_url"`
	Region    string `yaml:"region"`
}

// Validate rejects incomplete or unsupported publisher configurations.
func (c Config) Validate() error {
	switch c.Publisher {
	case "":
		if c.QueueURL != "" || c.Region != "" {
			return fmt.Errorf("apply_notifications.publisher is required when transport settings are present")
		}
	case "sqs":
		u, err := url.Parse(c.QueueURL)
		if err != nil || u.Host == "" || u.Scheme != "https" || u.Path == "" {
			return fmt.Errorf("apply_notifications.queue_url must be an HTTPS SQS queue URL")
		}
		if strings.HasSuffix(u.Path, ".fifo") {
			return fmt.Errorf("apply_notifications requires a standard SQS queue, not FIFO")
		}
	default:
		return fmt.Errorf("apply_notifications.publisher must be empty or sqs")
	}
	return nil
}

// Publisher returns nil only after the transport accepts the payload. Receivers
// deduplicate by event_id and correlate outcomes by the stable apply identifier;
// acceptance is not delivery.
type Publisher interface {
	Publish(context.Context, json.RawMessage) error
}

// NewPublisher constructs the selected transport using the normal AWS credential
// chain. Disabled delivery returns nil without loading AWS configuration.
func NewPublisher(ctx context.Context, c Config) (Publisher, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.Publisher == "" {
		return nil, nil
	}
	var opts []func(*config.LoadOptions) error
	if c.Region != "" {
		opts = append(opts, config.WithRegion(c.Region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration for completion notifications: %w", err)
	}
	return &sqsPublisher{client: sqs.NewFromConfig(cfg), queueURL: c.QueueURL}, nil
}

type sqsClient interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type sqsPublisher struct {
	client   sqsClient
	queueURL string
}

func (p *sqsPublisher) Publish(ctx context.Context, payload json.RawMessage) error {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(p.queueURL), MessageBody: aws.String(string(payload)),
	})
	if err != nil {
		return fmt.Errorf("send completion notification to SQS: %w", err)
	}
	return nil
}
