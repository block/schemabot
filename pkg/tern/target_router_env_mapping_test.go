package tern

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/etre"
	"github.com/block/schemabot/pkg/inventory"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// A sandbox plan and a later driver resume discover the same exact cluster
// through production inventory, retaining the logical environment and schema.
func TestTargetRouterInventoryEnvironmentMapping(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "dsid=orders-002,env=production", r.URL.Query().Get("query"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"writer_endpoint":"orders.example"}]`))
	}))
	t.Cleanup(srv.Close)
	client, err := etre.New(etre.Config{Addr: srv.URL, EntityType: "cluster"})
	require.NoError(t, err)
	resolver, err := etre.NewEtreResolver(etre.EtreResolverConfig{
		Client: client, TargetLabel: "dsid", EnvLabel: "env",
		EnvironmentMapping: map[string]string{"sandbox": "production"},
		HostField:          "writer_endpoint",
		Credentials:        inventory.SecretRefCredentialResolver{Username: "ddl", PasswordRef: "pw"},
		Assembler:          inventory.MySQLConnectionAssembler{DefaultPort: "3306"},
	})
	require.NoError(t, err)
	plan := &storage.Plan{
		ID: 7, PlanIdentifier: "plan-7", Target: "orders-002", Database: "orders",
		DatabaseType: "mysql", Environment: "sandbox",
		Namespaces: map[string]*storage.NamespacePlanData{"orders_sandbox": {}},
	}
	apply := &storage.Apply{
		ID: 42, ApplyIdentifier: "apply-42", PlanID: plan.ID, Database: "orders",
		DatabaseType: "mysql", Environment: "sandbox",
	}
	apply.SetOptions(storage.ApplyOptions{Target: "orders-002"})
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil,
		targetRouterPlanStore{byID: map[int64]*storage.Plan{7: plan}, byIdentifier: map[string]*storage.Plan{"plan-7": plan}}, created)
	_, err = router.Plan(ctx, &ternv1.PlanRequest{
		Database: "orders", Type: "mysql", Environment: "sandbox", Target: "orders-002",
		SchemaFiles: map[string]*ternv1.SchemaFiles{"orders_sandbox": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY)"}}},
	})
	require.NoError(t, err)
	local := created["orders"]
	require.NotNil(t, local)
	assert.Equal(t, "sandbox", local.planReq.Environment)
	assert.Equal(t, "orders-002", local.planReq.Target)
	assert.Contains(t, local.planReq.SchemaFiles, "orders_sandbox")
	assert.Empty(t, local.schemaOverrides)
	require.NoError(t, router.ResumeApply(ctx, apply))
	assert.Equal(t, "sandbox", local.resumeApply.Environment)
	assert.Equal(t, "orders-002", local.resumeApply.GetOptions().Target)
	assert.Contains(t, plan.Namespaces, "orders_sandbox")
}
