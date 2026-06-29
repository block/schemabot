//go:build integration

// Miscellaneous webhook integration tests (metrics, apply dual-write).

package webhook

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
)

// TestE2EApplyCreateDualWritesApplyOperationRow verifies the service-level
// apply create path writes exactly one apply_operations row in the same
// transaction as the applies row, mirroring the apply's (deployment, target)
// routing. The operator claim loop is not yet wired to these rows, so the
// row stays in pending — that's the contract until a subsequent PR lifts
// the deployments-map gate and introduces the per-row claim loop.
func TestE2EApplyCreateDualWritesApplyOperationRow(t *testing.T) {
	dbName := "webhook_apply_dual_write"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Seed the target so the plan produces a real DDL change.
	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

	schemaWithIndex := "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	planResp, err := svc.ExecutePlan(ctx, api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": schemaWithIndex}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.Changes, "expected DDL changes")

	applyResp, applyID, err := svc.ExecuteApply(ctx, api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)
	require.Greater(t, applyID, int64(0))

	apply, err := svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	plan, err := svc.Storage().Plans().Get(ctx, planResp.PlanID)
	require.NoError(t, err)
	require.NotNil(t, plan)

	ops, err := svc.Storage().ApplyOperations().ListByApply(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, ops, 1, "apply create must dual-write exactly one apply_operations row")
	op := ops[0]
	assert.Equal(t, applyID, op.ApplyID)
	assert.Equal(t, apply.Deployment, op.Deployment, "operation deployment must match apply deployment")
	assert.Equal(t, plan.Target, op.Target, "operation target must mirror the plan-time target")
	assert.Equal(t, state.ApplyOperation.Pending, op.State, "operation row stays pending — no consumer is wired yet")
}

func TestE2EWebhookMetrics(t *testing.T) {
	// Set up OTel ManualReader to capture metrics.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	dbName := "webhook_metrics_test"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// Send an issue_comment webhook (plan command).
	commentReq := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, commentReq)
	require.Equal(t, http.StatusOK, rr.Code)

	// Send a pull_request opened webhook (auto-plan trigger).
	prReq := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil)
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, prReq)
	require.Equal(t, http.StatusOK, rr2.Code)

	// Collect metrics and verify both webhook events were recorded.
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var found bool
	observedEvents := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "schemabot.webhook.events_total" {
				found = true
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok)

				for _, dp := range sum.DataPoints {
					evType, _ := dp.Attributes.Value(attribute.Key("event_type"))
					action, _ := dp.Attributes.Value(attribute.Key("action"))
					status, _ := dp.Attributes.Value(attribute.Key("status"))
					repo, _ := dp.Attributes.Value(attribute.Key("repository"))
					key := evType.AsString() + "/" + action.AsString()
					observedEvents[key] = true
					t.Logf("webhook metric: event_type=%s action=%s repo=%s status=%s",
						evType.AsString(), action.AsString(), repo.AsString(), status.AsString())
				}
			}
		}
	}
	require.True(t, found, "schemabot.webhook.events_total metric not found")
	assert.True(t, observedEvents["issue_comment/created"], "expected issue_comment/created metric")
	assert.True(t, observedEvents["pull_request/opened"], "expected pull_request/opened metric")
}
