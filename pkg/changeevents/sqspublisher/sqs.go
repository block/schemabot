// Package sqspublisher delivers change events to standard Amazon SQS queues.
package sqspublisher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/block/schemabot/pkg/changeevents"
)

// Destination selects which events a queue receives.
type Destination struct {
	QueueURL     string   `yaml:"queue_url"`
	Region       string   `yaml:"region,omitempty"`
	Environments []string `yaml:"environments"`
	Events       []string `yaml:"events"`
}

// NewDestinations builds publishers only for explicitly configured queues.
// changeevents starts a bounded, independently filtered worker for each one.
func NewDestinations(ctx context.Context, queues []Destination) ([]changeevents.PublisherDestination, error) {
	var destinations []changeevents.PublisherDestination
	for i, queue := range queues {
		u, err := url.Parse(queue.QueueURL)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Path == "" || strings.HasSuffix(u.Path, "/") {
			return nil, fmt.Errorf("sqs_change_events[%d].queue_url must be an HTTPS queue URL without credentials, query or fragment", i)
		}
		if strings.HasSuffix(u.Path, ".fifo") {
			return nil, fmt.Errorf("sqs_change_events[%d] requires a standard queue", i)
		}
		var opts []func(*config.LoadOptions) error
		if queue.Region != "" {
			opts = append(opts, config.WithRegion(queue.Region))
		}
		cfg, err := config.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("load AWS configuration for sqs_change_events[%d]: %w", i, err)
		}
		if cfg.Region == "" {
			return nil, fmt.Errorf("sqs_change_events[%d] requires a region in server or AWS configuration", i)
		}
		client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
			o.RetryMaxAttempts = 1
		})
		destinations = append(destinations, changeevents.PublisherDestination{
			ConfigPath: fmt.Sprintf("sqs_change_events[%d]", i),
			Events:     queue.Events, Environments: queue.Environments,
			Encode: encode, Publisher: &publisher{client: client, queueURL: queue.QueueURL},
		})
	}
	return destinations, nil
}

func encode(event changeevents.Event) (json.RawMessage, error) {
	return json.Marshal(struct {
		changeevents.EventDetails
		Type            string `json:"type"`
		App             string `json:"app"`
		Environment     string `json:"environment"`
		OccurredAtNanos int64  `json:"occurred_at_nanos"`
	}{
		EventDetails: event.Details, Type: event.Type, App: event.App,
		Environment: event.Environment, OccurredAtNanos: event.OccurredAtNanos,
	})
}

type publisher struct {
	client   *sqs.Client
	queueURL string
}

func (p *publisher) Publish(ctx context.Context, payload json.RawMessage) error {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(p.queueURL), MessageBody: aws.String(string(payload)),
	})
	if err != nil {
		return fmt.Errorf("send message to SQS queue %q: %w", p.queueURL, err)
	}
	return nil
}
