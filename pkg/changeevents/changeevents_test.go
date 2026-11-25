package changeevents

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const eventTestDeadline = 5 * time.Second

func TestSenderRendersAndFiltersEvents(t *testing.T) {
	requests := make(chan map[string]any, 3)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&body) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests <- body
		w.WriteHeader(http.StatusAccepted)
	}))
	defer endpoint.Close()

	sender, err := New([]Destination{{
		URL: endpoint.URL, Environments: []string{"staging"}, Events: []string{OperationStarted},
		Body: `{"app":{{json .App}},"ts":{{.OccurredAtNanos}},"description":{{json .Description}}}`,
	}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	defer sender.Close()

	sender.Offer(Event{Type: ApplyFinished, Environment: "staging"})
	sender.Offer(Event{Type: OperationStarted, Environment: "production"})
	event := Event{Type: OperationStarted, Environment: "staging", App: "orders", OccurredAtNanos: 1234, Description: "apply=\"quoted\"\noperation=42"}
	sender.Offer(event)

	select {
	case body := <-requests:
		require.Equal(t, map[string]any{"app": event.App, "ts": float64(1234), "description": event.Description}, body)
	case <-time.After(eventTestDeadline):
		t.Fatal("selected event did not reach the receiver")
	}
	require.Empty(t, sender.deliveries[0].queue)
}

func TestBlockedDestinationDropsWorkWithoutWaiting(t *testing.T) {
	received := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		received <- struct{}{}
		<-r.Context().Done()
		cancelled <- struct{}{}
	}))
	defer endpoint.Close()

	sender, err := New([]Destination{{
		URL: endpoint.URL, Environments: []string{"staging"}, Events: []string{OperationStarted}, Body: `{}`,
	}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	defer sender.Close()

	event := Event{Type: OperationStarted, Environment: "staging"}
	sender.Offer(event)
	select {
	case <-received:
	case <-time.After(eventTestDeadline):
		t.Fatal("receiver did not see the first request")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range queueCapacity + 5 {
			sender.Offer(event)
		}
	}()
	select {
	case <-done:
	case <-time.After(eventTestDeadline):
		t.Fatal("a full queue blocked the producer")
	}
	require.Equal(t, uint64(5), sender.deliveries[0].drops.Load())

	sender.Close()
	select {
	case <-cancelled:
	case <-time.After(eventTestDeadline):
		t.Fatal("closing the sender did not cancel its request")
	}
}

type publisherFunc func(context.Context, json.RawMessage) error

func (f publisherFunc) Publish(ctx context.Context, body json.RawMessage) error {
	return f(ctx, body)
}

func TestPublisherOnlyDestination(t *testing.T) {
	selected := []string{OperationStarted, OperationFinished, ApplyFinished}
	published := make(chan string, 8)
	sender, err := NewWithPublishers(nil, []PublisherDestination{{
		Events: selected, Environments: []string{"production"},
		Encode: func(e Event) (json.RawMessage, error) {
			return json.Marshal(e.Description)
		},
		Publisher: publisherFunc(func(ctx context.Context, payload json.RawMessage) error {
			published <- string(payload)
			return nil
		}),
	}}, slog.Default())
	require.NoError(t, err)
	defer sender.Close()

	for _, kind := range selected {
		require.True(t, sender.Selects(kind))
		require.True(t, sender.Enabled(kind, "production"))
		require.False(t, sender.Enabled(kind, "staging"))
		sender.Offer(Event{Type: kind, Environment: "staging", Description: "filtered"})
		sender.Offer(Event{Type: kind, Environment: "production", Description: kind})
	}
	for _, want := range selected {
		select {
		case got := <-published:
			require.JSONEq(t, strconv.Quote(want), got)
		case <-time.After(eventTestDeadline):
			t.Fatalf("publisher did not receive %s", want)
		}
	}
}

func TestDeferredOfferReturnsBeforeResolvingAndSharesResult(t *testing.T) {
	published := make(chan string, 2)
	dest := PublisherDestination{
		Events: []string{ApplyFinished}, Environments: []string{"staging"},
		Encode: func(event Event) (json.RawMessage, error) {
			return json.Marshal(event.Description)
		},
		Publisher: publisherFunc(func(_ context.Context, payload json.RawMessage) error {
			published <- string(payload)
			return nil
		}),
	}
	sender, err := NewWithPublishers(nil, []PublisherDestination{dest, dest}, slog.Default())
	require.NoError(t, err)
	t.Cleanup(sender.Close)

	var resolutions atomic.Int32
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	offered := make(chan struct{})
	go func() {
		defer close(offered)
		sender.OfferDeferred(ApplyFinished, "staging", func(ctx context.Context) (Event, bool) {
			resolutions.Add(1)
			started <- struct{}{}
			select {
			case <-release:
				return Event{Type: ApplyFinished, Environment: "staging", Description: "settled"}, true
			case <-ctx.Done():
				return Event{}, false
			}
		})
	}()

	deadline := time.After(eventTestDeadline)
	select {
	case <-started:
	case <-deadline:
		t.Fatal("workers did not start event preparation")
	}
	select {
	case <-offered:
	case <-deadline:
		t.Fatal("offering the event waited for preparation")
	}

	close(release)
	for range 2 {
		select {
		case payload := <-published:
			require.Equal(t, `"settled"`, payload)
		case <-deadline:
			t.Fatal("a destination did not receive the prepared event")
		}
	}
	require.EqualValues(t, 1, resolutions.Load(), "matching destinations share a single lookup")
}

func TestBlockedPublisherIsolatedFromHTTP(t *testing.T) {
	offers := make(chan struct{}, 2)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offers <- struct{}{}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer endpoint.Close()

	started := make(chan bool, 1)
	stopped := make(chan error, 1)
	sender, err := NewWithPublishers([]Destination{{
		URL: endpoint.URL, Events: []string{ApplyFinished}, Environments: []string{"staging"}, Body: "{}",
	}}, []PublisherDestination{{
		Events: []string{ApplyFinished}, Environments: []string{"staging"},
		Encode: func(e Event) (json.RawMessage, error) { return json.Marshal(e.Type) },
		Publisher: publisherFunc(func(ctx context.Context, _ json.RawMessage) error {
			_, bounded := ctx.Deadline()
			started <- bounded
			<-ctx.Done()
			stopped <- ctx.Err()
			return ctx.Err()
		}),
	}}, slog.Default())
	require.NoError(t, err)
	defer sender.Close()

	event := Event{Type: ApplyFinished, Environment: "staging"}
	sender.Offer(event)
	select {
	case bounded := <-started:
		require.True(t, bounded, "each publish must have a deadline")
	case <-time.After(eventTestDeadline):
		t.Fatal("injected publisher did not start")
	}
	sender.Offer(event)
	for range 2 {
		select {
		case <-offers:
		case <-time.After(eventTestDeadline):
			t.Fatal("blocked publisher stalled the HTTP destination")
		}
	}
	sender.Close()
	select {
	case err := <-stopped:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(eventTestDeadline):
		t.Fatal("closing the sender did not cancel the publisher")
	}
}

func TestPublisherWorkerContinuesAfterDeliveryErrors(t *testing.T) {
	published := make(chan string, 4)
	sender, err := NewWithPublishers(nil, []PublisherDestination{{
		Events: []string{ApplyFinished}, Environments: []string{"staging"},
		Encode: func(e Event) (json.RawMessage, error) {
			if e.Description == "" {
				return nil, errors.New("encoding failed")
			}
			return json.RawMessage(e.Description), nil
		},
		Publisher: publisherFunc(func(ctx context.Context, payload json.RawMessage) error {
			published <- string(payload)
			return errors.New("delivery failed")
		}),
	}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	defer sender.Close()
	for _, body := range []string{"", "invalid", "1", "2"} {
		sender.Offer(Event{Type: ApplyFinished, Environment: "staging", Description: body})
	}
	for _, want := range []string{"1", "2"} {
		select {
		case got := <-published:
			require.Equal(t, want, got)
		case <-time.After(eventTestDeadline):
			t.Fatalf("missing payload %s", want)
		}
	}
}
