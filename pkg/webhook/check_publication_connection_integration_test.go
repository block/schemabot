//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A slow GitHub request must leave storage capacity available to other work,
// even when the storage pool has only one connection.
func TestCheckPublicationLeavesStorageConnectionAvailable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause string
		run   func(context.Context, *Handler, *ghclient.InstallationClient) error
	}{
		{
			name: "aggregate fold", pause: "publish",
			run: func(ctx context.Context, h *Handler, client *ghclient.InstallationClient) error {
				_, err := h.updateAggregateCheckOnce(ctx, client, "octocat/hello-world", 1, "abc123")
				return err
			},
		},
		{
			name: "guard failure", pause: "publish",
			run: func(ctx context.Context, h *Handler, client *ghclient.InstallationClient) error {
				if !h.postFailingAggregatesWithBlock(ctx, client, "octocat/hello-world", 1, "abc123", map[string]string{"sandbox": "Planning failed"}, checkBlockReason{}) {
					return fmt.Errorf("guard failure was not published and stored")
				}
				return nil
			},
		},
		{
			name: "guard recovery", pause: "head",
			run: func(ctx context.Context, h *Handler, client *ghclient.InstallationClient) error {
				h.clearAggregateBlocksForVerifiedPR(ctx, client, "octocat/hello-world", 1, "abc123", nil)
				return nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			db, err := sql.Open("block-mysql", e2eSchemabotDSN)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			require.NoError(t, db.PingContext(ctx))
			db.SetMaxOpenConns(1)
			_, err = db.ExecContext(ctx, "DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
			require.NoError(t, err)
			svc := api.New(mysqlstore.New(db), prerequisiteConfig(), nil, testLogger())
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			for _, env := range []string{"sandbox", "production"} {
				require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
					Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123",
					Environment: env, DatabaseType: "mysql", DatabaseName: "orders",
					Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
				}))
			}

			client, mux := setupGitHubServer(t)
			install := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
			h := NewHandler(svc, &fakeClientFactory{client: install}, nil, testLogger())
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			pause := func(r *http.Request) {
				select {
				case entered <- struct{}{}:
				case <-r.Context().Done():
					return
				}
				select {
				case <-release:
				case <-r.Context().Done():
				}
			}
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
				if tc.pause == "head" {
					pause(r)
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"head": map[string]string{"sha": "abc123"}}))
			})
			serveAggregateScopeInputs(t, mux, []string{"schemabot.yaml"}, "database: orders\ntype: mysql\n")
			mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
				if tc.pause == "publish" {
					pause(r)
				}
				w.WriteHeader(http.StatusCreated)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]int64{"id": 555}))
			})
			result := make(chan error, 1)
			go func() { result <- tc.run(ctx, h, install) }()
			select {
			case <-entered:
			case err := <-result:
				t.Fatalf("publication returned before GitHub request paused: %v", err)
			case <-ctx.Done():
				t.Fatal("publication did not reach GitHub")
			}
			assert.Zero(t, db.Stats().InUse, "GitHub waits must not reserve a storage connection")
			queryCtx, queryCancel := context.WithTimeout(ctx, time.Second)
			defer queryCancel()
			checks, err := svc.Storage().Checks().GetByPR(queryCtx, "octocat/hello-world", 1)
			require.NoError(t, err, "other storage work can use the only pool connection")
			require.Len(t, checks, 2)
			unblock()
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("publication did not finish")
			}
		})
	}
}
