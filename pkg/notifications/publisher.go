package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Event identifies one completed apply. ApplyIdentifier is stable across retries.
type Event struct {
	ApplyIdentifier string    `json:"apply_identifier"`
	Database        string    `json:"database"`
	DatabaseType    string    `json:"database_type"`
	Environment     string    `json:"environment"`
	Repository      string    `json:"repository,omitempty"`
	PullRequest     int       `json:"pull_request,omitempty"`
	CompletedAt     time.Time `json:"completed_at"`
}

// Publisher sends a completion event. Receivers must deduplicate its identifier.
type Publisher interface {
	Publish(context.Context, Event) error
}

type sqsClient interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type sqsPublisher struct {
	client   sqsClient
	queueURL string
}

// NewPublisher selects the configured transport. Disabled config returns nil.
func NewPublisher(ctx context.Context, cfg Config) (Publisher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Type == "" {
		return nil, nil
	}
	opts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load SQS publisher AWS config: %w", err)
	}
	return &sqsPublisher{client: sqs.NewFromConfig(awsCfg), queueURL: cfg.QueueURL}, nil
}

func (p *sqsPublisher) Publish(ctx context.Context, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode apply completion %s: %w", event.ApplyIdentifier, err)
	}
	input := &sqs.SendMessageInput{QueueUrl: aws.String(p.queueURL), MessageBody: aws.String(string(body))}
	if strings.HasSuffix(p.queueURL, ".fifo") {
		input.MessageGroupId = aws.String("apply-completions")
		input.MessageDeduplicationId = aws.String(event.ApplyIdentifier)
	}
	if _, err := p.client.SendMessage(ctx, input); err != nil {
		return fmt.Errorf("send completion %s to SQS: %w", event.ApplyIdentifier, err)
	}
	return nil
}
