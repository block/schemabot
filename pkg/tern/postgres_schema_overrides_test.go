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
	progressResult *engine.ProgressResult
	pullRequest    *ternv1.PullSchemaRequest
}

func (e *namespaceCaptureEngine) Name() string { return "postgres" }

func (e *namespaceCaptureEngine) Plan(_ context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	e.planRequest = req
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
