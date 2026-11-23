package changeevents

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const exampleBody = `{"target":{{json .App}},"type":{{json .Type}},"time":{{.OccurredAtNanos}},"description":{{json .Description}}}`

func TestSenderFiltersAndRendersEvents(t *testing.T) {
	requests := make(chan map[string]any, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		var body map[string]any
		if assert.NoError(t, json.NewDecoder(r.Body).Decode(&body)) {
			requests <- body
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	sender, err := NewSender(t.Context(), []Destination{{
		URL: server.URL, Environments: []string{"test"}, Events: []Type{OperationStarted},
		Body: exampleBody,
	}}, slog.Default())
	require.NoError(t, err)
	t.Cleanup(sender.Close)

	sender.Offer(New(OperationStarted, "wrong environment", "other", "filtered"))
	sender.Offer(New(OperationFinished, "wrong type", "test", "filtered"))
	sender.Offer(Event{Type: OperationStarted, App: `app"name`, Environment: "test", OccurredAtNanos: 42, Description: "line 1\nline 2"})
	select {
	case req := <-requests:
		assert.Equal(t, map[string]any{
			"target": `app"name`, "type": "operation.started", "time": float64(42),
			"description": "line 1\nline 2",
		}, req)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the event")
	}
}

func TestBlockedDestinationDropsWithoutBlockingOtherDestinations(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(blocked.Close)
	t.Cleanup(func() { close(release) })
	delivered := make(chan struct{}, 1)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		delivered <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(healthy.Close)

	sender, err := NewSender(t.Context(), []Destination{
		{URL: blocked.URL, Environments: []string{"test"}, Events: []Type{OperationStarted}, Body: exampleBody},
		{URL: healthy.URL, Environments: []string{"test"}, Events: []Type{ApplyFinished}, Body: exampleBody},
	}, slog.Default())
	require.NoError(t, err)
	t.Cleanup(sender.Close)

	event := New(OperationStarted, "app", "test", "blocked")
	sender.Offer(event)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the blocked request")
	}
	offered := make(chan struct{})
	go func() {
		for range queueSize + 1 {
			sender.Offer(event)
		}
		sender.Offer(New(ApplyFinished, "app", "test", "healthy"))
		close(offered)
	}()
	select {
	case <-offered:
	case <-time.After(time.Second):
		t.Fatal("producer blocked on a full queue")
	}
	assert.Positive(t, sender.destinations[0].dropped.Load())
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("one blocked destination held up the other destination")
	}
	sender.Close()
	require.ErrorIs(t, sender.ctx.Err(), context.Canceled)
}
