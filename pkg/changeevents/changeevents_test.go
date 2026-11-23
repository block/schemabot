package changeevents

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
