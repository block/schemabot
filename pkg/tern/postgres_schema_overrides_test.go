package tern

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

type namespaceCaptureEngine struct {
	engine.Engine
	planRequest    *engine.PlanRequest
	applyRequest   *engine.ApplyRequest
	planNamespace  string
	planResult     *engine.PlanResult
	progressResult *engine.ProgressResult
	pullRequest    *ternv1.PullSchemaRequest
}

func (e *namespaceCaptureEngine) Name() string { return "postgres" }

func (e *namespaceCaptureEngine) Plan(_ context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	e.planRequest = req
	if e.planResult != nil {
		return e.planResult, nil
	}
	return &engine.PlanResult{PlanID: "plan", Changes: []engine.SchemaChange{{Namespace: e.planNamespace}}}, nil
}

func (e *namespaceCaptureEngine) Apply(_ context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applyRequest = req
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *namespaceCaptureEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return e.progressResult, nil
}

func (e *namespaceCaptureEngine) PullSchema(_ context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	e.pullRequest = req
	return &ternv1.PullSchemaResponse{
		Namespaces: map[string]*ternv1.PulledNamespace{req.GetNamespace(): {Tables: map[string]string{"users.sql": "CREATE TABLE users (id bigint);"}}},
	}, nil
}

func postgresOverrideClient(fake *namespaceCaptureEngine) *LocalClient {
	return &LocalClient{
		config: LocalConfig{
			Database:        "app",
			Type:            storage.DatabaseTypePostgres,
			SchemaOverrides: map[string]string{"svc": "svc-database-qa"},
		},
		postgresEngine: fake,
		logger:         slog.Default(),
	}
}

func TestPostgresPlanRemapsNamespacesAtEngineBoundary(t *testing.T) {
	fake := &namespaceCaptureEngine{planNamespace: "svc-database-qa"}
	client := postgresOverrideClient(fake)
	files := schema.SchemaFiles{"svc": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint);"}}}

	result, err := client.planWithEngine(t.Context(), &ternv1.PlanRequest{}, "app", files)
	require.NoError(t, err)
	assert.Contains(t, fake.planRequest.SchemaFiles, "svc-database-qa")
	assert.NotContains(t, fake.planRequest.SchemaFiles, "svc")
	require.Len(t, result.Changes, 1)
	assert.Equal(t, "svc", result.Changes[0].Namespace)
}

func TestPostgresApplyAndProgressRemapNamespacesAtEngineBoundary(t *testing.T) {
	fake := &namespaceCaptureEngine{progressResult: &engine.ProgressResult{
		Tables: []engine.TableProgress{{Namespace: "svc-database-qa", Table: "users"}},
	}}
	client := postgresOverrideClient(fake)

	_, err := client.applyWithEngine(t.Context(), fake, &engine.ApplyRequest{
		Changes:     []engine.SchemaChange{{Namespace: "svc"}},
		SchemaFiles: schema.SchemaFiles{"svc": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint);"}}},
	})
	require.NoError(t, err)
	require.Len(t, fake.applyRequest.Changes, 1)
	assert.Equal(t, "svc-database-qa", fake.applyRequest.Changes[0].Namespace)
	assert.Contains(t, fake.applyRequest.SchemaFiles, "svc-database-qa")

	progress, err := client.progressWithEngine(t.Context(), fake, &engine.ProgressRequest{})
	require.NoError(t, err)
	require.Len(t, progress.Tables, 1)
	assert.Equal(t, "svc", progress.Tables[0].Namespace)
}

func TestPostgresPullRemapsRequestedAndDiscoveredNamespace(t *testing.T) {
	for _, namespace := range []string{"svc", ""} {
		t.Run(namespace, func(t *testing.T) {
			fake := &namespaceCaptureEngine{}
			client := postgresOverrideClient(fake)

			response, err := client.pullSchemaFromEngine(t.Context(), &ternv1.PullSchemaRequest{Namespace: namespace})
			require.NoError(t, err)
			assert.Equal(t, "svc-database-qa", fake.pullRequest.Namespace)
			assert.Contains(t, response.Namespaces, "svc")
			assert.NotContains(t, response.Namespaces, "svc-database-qa")
		})
	}
}

func TestPostgresSchemaOverrideRejectsUnmappedCanonicalNamespace(t *testing.T) {
	fake := &namespaceCaptureEngine{}
	client := postgresOverrideClient(fake)

	_, err := client.applyWithEngine(t.Context(), fake, &engine.ApplyRequest{
		Changes: []engine.SchemaChange{{Namespace: "other"}},
	})
	require.ErrorContains(t, err, `target does not authorize namespace "other"`)
	assert.Nil(t, fake.applyRequest)
}

func TestPostgresSchemaOverrideRejectsUnknownPhysicalNamespace(t *testing.T) {
	fake := &namespaceCaptureEngine{planNamespace: "unexpected"}
	client := postgresOverrideClient(fake)

	_, err := client.planWithEngine(t.Context(), &ternv1.PlanRequest{}, "app", schema.SchemaFiles{"svc": {}})
	require.ErrorContains(t, err, `engine returned unauthorized physical namespace "unexpected"`)
}

// A plan with nothing to change can still carry disclosures — tables exempt
// from the verdict, unfinished work an earlier schema change left behind — and
// each names the namespace the engine read. Those come back canonical too, so
// the review surface never shows the physical schema, and an unmapped one is
// refused the same way an unmapped change is.
func TestPostgresPlanCanonicalizesDisclosureNamespaces(t *testing.T) {
	disclosureOnlyPlan := func(exemptNamespace string) *engine.PlanResult {
		return &engine.PlanResult{
			PlanID:    "plan",
			NoChanges: true,
			ExemptTables: []*engine.ExemptTables{
				{Namespace: exemptNamespace, Tables: []string{"users_archive"}, Reason: "archive naming"},
			},
			ExistingCopies: []*engine.ExistingCopy{
				{Namespace: "svc-database-qa", Tables: []string{"users"}, Disposition: engine.CopyAdopt},
			},
		}
	}

	fake := &namespaceCaptureEngine{planResult: disclosureOnlyPlan("svc-database-qa")}
	client := postgresOverrideClient(fake)
	result, err := client.planWithEngine(t.Context(), &ternv1.PlanRequest{}, "app", schema.SchemaFiles{"svc": {}})
	require.NoError(t, err)
	require.Len(t, result.ExemptTables, 1)
	assert.Equal(t, "svc", result.ExemptTables[0].Namespace)
	assert.Equal(t, []string{"users_archive"}, result.ExemptTables[0].Tables)
	require.Len(t, result.ExistingCopies, 1)
	assert.Equal(t, "svc", result.ExistingCopies[0].Namespace)

	fake = &namespaceCaptureEngine{planResult: disclosureOnlyPlan("unexpected")}
	client = postgresOverrideClient(fake)
	_, err = client.planWithEngine(t.Context(), &ternv1.PlanRequest{}, "app", schema.SchemaFiles{"svc": {}})
	require.ErrorContains(t, err, `canonicalize exempt tables namespace: engine returned unauthorized physical namespace "unexpected"`)
}

// Progress is a report on an apply that is already running, so its namespace
// labels never fail the poll: an unset label stays unset, and a label naming a
// schema this target does not map is passed through as the engine reported it
// rather than turning a healthy apply into an error.
func TestPostgresProgressToleratesUnmappableTableLabels(t *testing.T) {
	fake := &namespaceCaptureEngine{progressResult: &engine.ProgressResult{
		State: engine.StateRunning,
		Tables: []engine.TableProgress{
			{Namespace: "svc-database-qa", Table: "users"},
			{Namespace: "", Table: "orders"},
			{Namespace: "unexpected", Table: "events"},
		},
	}}
	client := postgresOverrideClient(fake)

	progress, err := client.progressWithEngine(t.Context(), fake, &engine.ProgressRequest{})
	require.NoError(t, err)
	require.Len(t, progress.Tables, 3)
	assert.Equal(t, "svc", progress.Tables[0].Namespace)
	assert.Equal(t, "", progress.Tables[1].Namespace)
	assert.Equal(t, "unexpected", progress.Tables[2].Namespace)
	assert.Equal(t, engine.StateRunning, progress.State)
}

// Two canonical namespaces cannot share a physical schema on one apply: the
// engine keys its schema files by physical name, so a collision would silently
// drop one namespace's files. The single-mapping limit makes this unreachable
// from configuration; the guard pins it for a schema-files map that names the
// physical schema directly beside the canonical one.
func TestPostgresPhysicalSchemaFilesRejectCollision(t *testing.T) {
	client := postgresOverrideClient(&namespaceCaptureEngine{})
	client.config.SchemaOverrides = map[string]string{"svc": "svc-database-qa", "svc-database-qa": "svc-database-qa"}

	_, err := client.physicalSchemaFiles(schema.SchemaFiles{"svc": {}, "svc-database-qa": {}})
	require.ErrorContains(t, err, `schema files map multiple namespaces to physical namespace "svc-database-qa"`)
}
