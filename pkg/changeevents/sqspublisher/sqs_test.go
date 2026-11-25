package sqspublisher

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/block/schemabot/pkg/changeevents"
	"github.com/stretchr/testify/require"
)

const queueURL = "https://sqs.us-east-1.amazonaws.com/123456789012/schema-events"

func useEndpoint(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	endpoint := httptest.NewServer(handler)
	t.Cleanup(endpoint.Close)
	t.Setenv("AWS_ENDPOINT_URL_SQS", endpoint.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func TestSQSDelivery(t *testing.T) {
	for _, withHTTP := range []bool{false, true} {
		t.Run(fmt.Sprintf("withHTTP=%t", withHTTP), func(t *testing.T) {
			type message struct {
				QueueURL string `json:"QueueUrl"`
				Body     string `json:"MessageBody"`
			}
			messages := make(chan message, 4)
			useEndpoint(t, func(w http.ResponseWriter, r *http.Request) {
				var m message
				if r.Header.Get("X-Amz-Target") != "AmazonSQS.SendMessage" || json.NewDecoder(r.Body).Decode(&m) != nil {
					http.Error(w, "expected SendMessage", http.StatusBadRequest)
					return
				}
				messages <- m
				_, _ = fmt.Fprint(w, "{}")
			})
			queues, err := NewDestinations(t.Context(), []Destination{{
				QueueURL: queueURL, Environments: []string{"prod"}, Events: []string{changeevents.ApplyFinished},
			}})
			require.NoError(t, err)
			var hooks []changeevents.Destination
			httpCalls := make(chan struct{}, 4)
			if withHTTP {
				httpDest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					httpCalls <- struct{}{}
				}))
				t.Cleanup(httpDest.Close)
				hooks = append(hooks, changeevents.Destination{URL: httpDest.URL, Environments: []string{"prod"},
					Events: []string{changeevents.ApplyFinished}, Body: "{}"})
			}
			sender, err := changeevents.NewWithPublishers(hooks, queues, slog.Default())
			require.NoError(t, err)
			t.Cleanup(sender.Close)
			sender.Offer(changeevents.Event{Type: changeevents.OperationStarted, Environment: "prod"})
			sender.Offer(changeevents.Event{Type: changeevents.ApplyFinished, Environment: "dev"})
			sender.Offer(changeevents.Event{Type: changeevents.ApplyFinished, App: "orders", Environment: "prod", OccurredAtNanos: 42,
				Details: changeevents.EventDetails{EventID: "event-1", ApplyID: "apply_1", Database: "db", DatabaseType: "mysql",
					Outcome: "failed", CompletedAt: time.Unix(1, 0).UTC()}})
			select {
			case m := <-messages:
				require.Equal(t, queueURL, m.QueueURL)
				require.JSONEq(t, `{"event_id":"event-1","id":"apply_1","database":"db","database_type":"mysql","outcome":"failed","completed_at":"1970-01-01T00:00:01Z","type":"apply.finished","app":"orders","environment":"prod","occurred_at_nanos":42}`, m.Body)
			case <-time.After(5 * time.Second):
				t.Fatal("missing SQS message")
			}
			if withHTTP {
				select {
				case <-httpCalls:
				case <-time.After(5 * time.Second):
					t.Fatal("missing HTTP request")
				}
			}
		})
	}
}

func TestOperationPayload(t *testing.T) {
	event := changeevents.Event{
		Type: changeevents.OperationFinished, App: "orders", Environment: "prod", OccurredAtNanos: 42,
		Description: "worker summary",
		Details: changeevents.EventDetails{
			EventID: "event-2", ApplyID: "apply_1", Database: "db", DatabaseType: "mysql",
			Repository: "example/schemas", PullRequest: 7, OperationID: 3, Deployment: "shard-a",
			Attempt: 2, Outcome: "failed_retryable",
		},
	}
	payload, err := encode(event)
	require.NoError(t, err)
	require.JSONEq(t, `{"event_id":"event-2","id":"apply_1","database":"db","database_type":"mysql","repository":"example/schemas","pull_request":7,"operation_id":3,"deployment":"shard-a","attempt":2,"outcome":"failed_retryable","type":"operation.finished","app":"orders","environment":"prod","occurred_at_nanos":42}`, string(payload))

	// Existing HTTP templates may encode the entire Event with {{json .}}.
	httpBody, err := json.Marshal(event)
	require.NoError(t, err)
	require.JSONEq(t, `{"Type":"operation.finished","App":"orders","Environment":"prod","OccurredAtNanos":42,"Description":"worker summary"}`, string(httpBody))
}

func TestSQSPublishReturnsFirstFailure(t *testing.T) {
	var attempts atomic.Int32
	useEndpoint(t, func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	})
	queues, err := NewDestinations(t.Context(), []Destination{{QueueURL: queueURL}})
	require.NoError(t, err)
	err = queues[0].Publisher.Publish(t.Context(), json.RawMessage("{}"))
	require.ErrorContains(t, err, queueURL)
	var responseErr *awshttp.ResponseError
	require.ErrorAs(t, err, &responseErr)
	require.Equal(t, http.StatusServiceUnavailable, responseErr.HTTPStatusCode())
	require.EqualValues(t, 1, attempts.Load())
}

func TestSQSConfig(t *testing.T) {
	t.Setenv("AWS_PROFILE", "missing-profile")
	queues, err := NewDestinations(t.Context(), nil)
	require.NoError(t, err)
	require.Empty(t, queues)
	for _, invalid := range []string{"", "http://example.com/queue", queueURL + ".fifo", "https://example.com/", queueURL + "?token=secret"} {
		_, err := NewDestinations(t.Context(), []Destination{{QueueURL: invalid}})
		require.ErrorContains(t, err, "sqs_change_events[0]")
	}
}

func TestSQSFilterErrorWithHTTPDestination(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	queues, err := NewDestinations(t.Context(), []Destination{{
		QueueURL: queueURL, Environments: []string{"prod"}, Events: []string{"apply.unknown"},
	}})
	require.NoError(t, err)
	hooks := []changeevents.Destination{{
		URL: "https://example.com/hook", Environments: []string{"prod"},
		Events: []string{changeevents.ApplyFinished}, Body: "{}",
	}}
	_, err = changeevents.NewWithPublishers(hooks, queues, slog.Default())
	require.EqualError(t, err, `sqs_change_events[0]: unknown event "apply.unknown"`)
}
