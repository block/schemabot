package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInitRequestsCancelInFlight(t *testing.T) {
	for _, kind := range []string{"pull", "plan"} {
		t.Run(kind, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "schema.sql"), []byte("CREATE TABLE users (id bigint);"), 0600))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if kind == "pull" {
					_, err := CallPullSchemaAPIWithContext(ctx, server.URL, "shop", "postgres", "dev", PullSchemaOptions{})
					done <- err
				} else {
					_, _, err := CallPlanAPIWithContext(ctx, server.URL, "shop", "postgres", "dev", dir, "", 0, nil, false)
					done <- err
				}
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("request never started")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("request did not stop promptly")
			}
		})
	}
}
