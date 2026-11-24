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
		if err := validateSQSQueue(c.QueueURL); err != nil {
			return fmt.Errorf("apply_notifications: %w", err)
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
	resolved, err := resolveSQSPublisher(ctx, c.QueueURL, c.Region)
	if err != nil {
		return nil, err
	}
	return resolved.Publisher, nil
}

func validateSQSQueue(queueURL string) error {
	u, err := url.Parse(queueURL)
	if err != nil || u.Host == "" || u.Scheme != "https" || u.Path == "" {
		return fmt.Errorf("queue_url must be an HTTPS SQS queue URL")
	}
	if strings.HasSuffix(u.Path, ".fifo") {
		return fmt.Errorf("publisher requires a standard SQS queue, not FIFO")
	}
	return nil
}

func resolveSQSPublisher(ctx context.Context, queueURL, region string) (ResolvedPublisher, error) {
	var opts []func(*config.LoadOptions) error
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return ResolvedPublisher{}, fmt.Errorf("load AWS configuration for completion notifications: %w", err)
	}
	if cfg.Region == "" {
		return ResolvedPublisher{}, fmt.Errorf("resolve SQS publisher: AWS region is required")
	}
	// Use the same resolved AWS configuration for identity and client creation.
	identity, err := json.Marshal([]string{queueURL, cfg.Region})
	if err != nil {
		return ResolvedPublisher{}, fmt.Errorf("encode SQS destination identity: %w", err)
	}
	return ResolvedPublisher{
		Identity:  string(identity),
		Publisher: &sqsPublisher{client: sqs.NewFromConfig(cfg), queueURL: queueURL},
	}, nil
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
