package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// appliedHistoryStorage serves a fixed apply history: the applies of the
// database, each apply's operations, and the plans they ran.
type appliedHistoryStorage struct {
	storage.Storage
	applies []*storage.Apply
	ops     map[int64][]*storage.ApplyOperation
	plans   map[int64]*storage.Plan
}

func (s *appliedHistoryStorage) Applies() storage.ApplyStore {
	return appliedHistoryApplies{applies: s.applies}
}

func (s *appliedHistoryStorage) ApplyOperations() storage.ApplyOperationStore {
	return appliedHistoryOps{ops: s.ops}
}

func (s *appliedHistoryStorage) Plans() storage.PlanStore {
	return appliedHistoryPlans{plans: s.plans}
}

type appliedHistoryApplies struct {
	storage.ApplyStore
	applies []*storage.Apply
}

func (a appliedHistoryApplies) GetByDatabase(context.Context, string, string, string) ([]*storage.Apply, error) {
	return a.applies, nil
}

type appliedHistoryOps struct {
	storage.ApplyOperationStore
	ops map[int64][]*storage.ApplyOperation
}

func (o appliedHistoryOps) ListByApply(_ context.Context, applyID int64) ([]*storage.ApplyOperation, error) {
	return o.ops[applyID], nil
}

type appliedHistoryPlans struct {
	storage.PlanStore
	plans map[int64]*storage.Plan
}

func (p appliedHistoryPlans) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	return p.plans[id], nil
}

func appliedNamespace(file, content string) *schema.Namespace {
	return &schema.Namespace{Files: map[string]string{file: content}}
}

// The schema files a plan compares against are, for each namespace the
// plan covers, the files of the apply that last completed it on this
// deployment. An apply that changed only another namespace, an apply that
// ran only on another deployment, a newer apply that failed, and a
// namespace the plan no longer covers leave that answer alone.
func TestAppliedSchemaFilesTakesEachNamespaceFromItsLastCompletedApply(t *testing.T) {
	now := time.Now()
	at := func(ago time.Duration) *time.Time {
		completed := now.Add(-ago)
		return &completed
	}
	publicV1 := appliedNamespace("users.sql", "CREATE TABLE users (id bigint) -- v1")
	publicV2 := appliedNamespace("users.sql", "CREATE TABLE users (id bigint) -- v2")
	audit := appliedNamespace("log.sql", "CREATE TABLE log (id bigint)")
	retired := appliedNamespace("old.sql", "CREATE TABLE old (id bigint)")

	history := &appliedHistoryStorage{
		applies: []*storage.Apply{
			// Created last, failed: never the files last applied.
			{ID: 5, ApplyIdentifier: "apply-failed", PlanID: 50, State: state.Apply.Failed},
			// Completed last, but changed only the audit namespace.
			{ID: 4, ApplyIdentifier: "apply-audit", PlanID: 40, State: state.Apply.Completed, CompletedAt: at(time.Minute)},
			// Completed after public v2, on another deployment only.
			{ID: 3, ApplyIdentifier: "apply-elsewhere", PlanID: 30, State: state.Apply.Completed, CompletedAt: at(2 * time.Minute)},
			// Created before public v2 but completed after it: still the
			// latest for public, by completion time.
			{ID: 1, ApplyIdentifier: "apply-public-v2", PlanID: 10, State: state.Apply.Completed, CompletedAt: at(3 * time.Minute)},
			{ID: 2, ApplyIdentifier: "apply-public-v1", PlanID: 20, State: state.Apply.Completed, CompletedAt: at(time.Hour)},
		},
		ops: map[int64][]*storage.ApplyOperation{
			3: {{ApplyID: 3, Deployment: "other", PlanID: 30}},
			// A rollout operation on this deployment runs its own plan.
			1: {{ApplyID: 1, Deployment: "testapp", PlanID: 11}, {ApplyID: 1, Deployment: "other", PlanID: 12}},
		},
		plans: map[int64]*storage.Plan{
			50: {SchemaFiles: schema.SchemaFiles{"public": appliedNamespace("users.sql", "failed")}},
			40: {SchemaFiles: schema.SchemaFiles{"audit": audit}},
			30: {SchemaFiles: schema.SchemaFiles{"public": appliedNamespace("users.sql", "elsewhere")}},
			10: {SchemaFiles: schema.SchemaFiles{"public": appliedNamespace("users.sql", "apply plan, not this deployment's")}},
			11: {SchemaFiles: schema.SchemaFiles{"public": publicV2}},
			20: {SchemaFiles: schema.SchemaFiles{"public": publicV1, "retired": retired}},
		},
	}
	client := &LocalClient{
		config:  LocalConfig{Database: "testapp", Type: storage.DatabaseTypePostgres},
		storage: history,
		logger:  slog.Default(),
	}

	desired := schema.SchemaFiles{"public": publicV2, "audit": audit}
	applied, err := client.appliedSchemaFiles(t.Context(), "staging", desired)
	require.NoError(t, err)
	assert.Equal(t, schema.SchemaFiles{"public": publicV2, "audit": audit}, applied)

	none, err := client.appliedSchemaFiles(t.Context(), "staging", schema.SchemaFiles{"billing": audit})
	require.NoError(t, err)
	assert.Nil(t, none, "a namespace no completed apply covered has no files last applied")
}

// A plan reads only the newest completed applies for the files last applied.
// A namespace that only an older apply covered is treated as never applied,
// so a plan never walks the whole history.
func TestAppliedSchemaFilesReadsOnlyTheNewestApplies(t *testing.T) {
	now := time.Now()
	history := &appliedHistoryStorage{plans: map[int64]*storage.Plan{}}
	for i := range appliedHistoryWindow + 1 {
		id := int64(i + 1)
		completed := now.Add(-time.Duration(i) * time.Minute)
		history.applies = append(history.applies, &storage.Apply{
			ID: id, ApplyIdentifier: fmt.Sprintf("apply-%d", id), PlanID: id,
			State: state.Apply.Completed, CompletedAt: &completed,
		})
		history.plans[id] = &storage.Plan{SchemaFiles: schema.SchemaFiles{"public": appliedNamespace("users.sql", fmt.Sprintf("-- v%d", id))}}
	}
	oldest := history.applies[appliedHistoryWindow]
	history.plans[oldest.PlanID].SchemaFiles["billing"] = appliedNamespace("invoices.sql", "CREATE TABLE invoices (id bigint)")
	client := &LocalClient{
		config:  LocalConfig{Database: "testapp", Type: storage.DatabaseTypePostgres},
		storage: history,
		logger:  slog.Default(),
	}

	applied, err := client.appliedSchemaFiles(t.Context(), "staging", schema.SchemaFiles{
		"public":  appliedNamespace("users.sql", ""),
		"billing": appliedNamespace("invoices.sql", ""),
	})

	require.NoError(t, err)
	assert.Equal(t, schema.SchemaFiles{"public": appliedNamespace("users.sql", "-- v1")}, applied,
		"billing, covered only by an apply past the window, reads as never applied")
}

// historyUnavailableStorage fails every read of apply history.
type historyUnavailableStorage struct{ storage.Storage }

func (historyUnavailableStorage) Applies() storage.ApplyStore { return historyUnavailableApplies{} }

type historyUnavailableApplies struct{ storage.ApplyStore }

func (historyUnavailableApplies) GetByDatabase(context.Context, string, string, string) ([]*storage.Apply, error) {
	return nil, errors.New("apply history unavailable")
}

// Only PostgreSQL reads the files last applied, so a Vitess plan neither pays
// for the history nor fails when it cannot be read.
func TestPlanWithEngineReadsAppliedHistoryOnlyForPostgres(t *testing.T) {
	var seen *engine.PlanRequest
	eng := fakePlanEngine{planFn: func(_ context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
		seen = req
		return &engine.PlanResult{NoChanges: true}, nil
	}}
	files := schema.SchemaFiles{"commerce": appliedNamespace("users.sql", "CREATE TABLE users (id bigint)")}
	req := &ternv1.PlanRequest{Environment: "staging"}

	vitess := &LocalClient{
		config:            LocalConfig{Database: "testapp", Type: storage.DatabaseTypeVitess},
		planetscaleEngine: eng,
		storage:           historyUnavailableStorage{},
		logger:            slog.Default(),
	}
	_, err := vitess.planWithEngine(t.Context(), req, "testapp", files)
	require.NoError(t, err, "a Vitess plan does not read apply history")
	require.NotNil(t, seen)
	assert.Nil(t, seen.AppliedSchemaFiles)

	postgres := &LocalClient{
		config:         LocalConfig{Database: "testapp", Type: storage.DatabaseTypePostgres},
		postgresEngine: eng,
		storage:        historyUnavailableStorage{},
		logger:         slog.Default(),
	}
	_, err = postgres.planWithEngine(t.Context(), req, "testapp", files)
	require.ErrorContains(t, err, "apply history unavailable", "a PostgreSQL plan fails closed without its history")
}
