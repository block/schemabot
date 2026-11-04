//go:build integration

package webhook

import (
	"context"
	"testing"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pendingPlanFailingStore struct{ storage.CheckStore }

func (pendingPlanFailingStore) MarkPlanPending(context.Context, *storage.Check) error {
	return assert.AnError
}

// A failed plan could not replace an older success on the same commit. Retry
// preparation must leave the gate blocked until replacement results are saved,
// even if it crashes before planning or cannot invalidate the old evidence.
func TestAggregateReplanRequiresReplacementEvidence(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		for _, invalidationFails := range []bool{false, true} {
			name := "global"
			if scoped {
				name = "scoped"
			}
			if invalidationFails {
				name += "_storage_failure"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				cfg := prerequisiteConfig()
				if !scoped {
					cfg.AllowedEnvironments = nil
				}
				svc := setupE2EServiceWithConfig(t, cfg)
				checks := svc.Storage().Checks()
				rows := []*storage.Check{}
				for _, env := range []string{"sandbox", "production"} {
					row := &storage.Check{Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123", Environment: env, DatabaseType: "mysql", DatabaseName: "orders", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}
					require.NoError(t, checks.Upsert(ctx, row))
					rows = append(rows, row)
				}
				env := aggregateSentinel
				if scoped {
					env = "production"
				}
				require.NoError(t, checks.Upsert(ctx, &storage.Check{Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123", Environment: env, DatabaseType: aggregateSentinel, DatabaseName: aggregateSentinel, Status: checkStatusCompleted, Conclusion: checkConclusionFailure, BlockingReason: planFailedUnrecordedBlock.blockingReason}))
				handlerChecks := checks
				if invalidationFails {
					handlerChecks = pendingPlanFailingStore{checks}
				}
				h, mux, client := newFoldHandler(t, cfg, handlerChecks)
				serveHeadSHA(t, mux, "abc123")
				serveAggregateScopeInputs(t, mux, []string{"schemabot.yaml"}, "database: orders\ntype: mysql\n")
				serveCheckRunCreate(t, mux)
				serveParticipantCheckRunsEmpty(t, mux, "abc123")
				configs := []ghclient.DiscoveredConfig{{Config: &ghclient.SchemabotConfig{Database: "orders", Type: ghclient.DatabaseTypeMySQL}}}
				h.clearAggregateBlocksForVerifiedPR(ctx, client, "octocat/hello-world", 1, "abc123", configs)
				assertGate := func(wantSuccess bool) {
					t.Helper()
					_, err := h.updateAggregateCheckOnce(ctx, client, "octocat/hello-world", 1, "abc123")
					require.NoError(t, err)
					row, err := checks.Get(ctx, "octocat/hello-world", 1, env, aggregateSentinel, aggregateSentinel)
					require.NoError(t, err)
					require.NotNil(t, row)
					assert.Equal(t, wantSuccess, row.Conclusion == checkConclusionSuccess)
				}
				// Both the immediate fold and a later recovery fold remain blocked if
				// the process never gets as far as storing a new plan.
				assertGate(false)
				assertGate(false)
				if invalidationFails {
					row, err := checks.Get(ctx, "octocat/hello-world", 1, env, aggregateSentinel, aggregateSentinel)
					require.NoError(t, err)
					assert.Equal(t, planFailedUnrecordedBlock.blockingReason, row.BlockingReason)
					return
				}
				for _, row := range rows {
					stored, err := checks.UpsertPlanResult(ctx, row, storage.PlanDriftClean)
					require.NoError(t, err)
					require.True(t, stored)
					if row.Environment == "sandbox" {
						assertGate(false)
					}
				}
				assertGate(true)
			})
		}
	}
}
