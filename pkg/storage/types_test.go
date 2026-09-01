package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyOptionsFromMapRoundTrip verifies that user-facing apply options can
// be decoded from API-style strings and encoded back without losing fields.
func TestApplyOptionsFromMapRoundTrip(t *testing.T) {
	options := ApplyOptionsFromMap(map[string]string{
		"allow_unsafe":  "true",
		"branch":        "schema-change-branch",
		"defer_cutover": "true",
		"defer_deploy":  "true",
		"skip_revert":   "true",
	})

	assert.Equal(t, ApplyOptions{
		AllowUnsafe:  true,
		Branch:       "schema-change-branch",
		DeferCutover: true,
		DeferDeploy:  true,
		SkipRevert:   true,
	}, options)

	assert.Equal(t, map[string]string{
		"allow_unsafe":  "true",
		"branch":        "schema-change-branch",
		"defer_cutover": "true",
		"defer_deploy":  "true",
		"skip_revert":   "true",
	}, options.Map())
}

func TestPlanUnsafeDDLChanges(t *testing.T) {
	plan := &Plan{Namespaces: map[string]*NamespacePlanData{
		"testdb": {Tables: []TableChange{
			{Namespace: "testdb", Table: "users", Operation: "alter", IsUnsafe: true, UnsafeReason: "DROP COLUMN removes data"},
			{Namespace: "testdb", Table: "orders", Operation: "drop"},
			{Namespace: "testdb", Table: "products", Operation: "alter"},
		}},
	}, Shards: []ShardPlan{{
		Namespace: "testdb",
		Shard:     "-80",
		Changes:   []TableChange{{Table: "accounts", Operation: "alter", IsUnsafe: true, UnsafeReason: "DROP PRIMARY KEY rebuilds the table"}},
	}}}

	changes := plan.UnsafeDDLChanges()

	require.Len(t, changes, 3)
	assert.Equal(t, "users", changes[0].Table)
	assert.Equal(t, "DROP COLUMN removes data", changes[0].UnsafeOptInReason())
	assert.Equal(t, "orders", changes[1].Table)
	assert.Equal(t, "DROP TABLE removes all data", changes[1].UnsafeOptInReason())
	assert.Equal(t, "accounts", changes[2].Table)
	assert.Equal(t, "testdb", changes[2].Namespace)
	assert.Equal(t, "DROP PRIMARY KEY rebuilds the table", changes[2].UnsafeOptInReason())
}

// TestPlanBlockedChanges verifies blocked-verdict extraction across
// namespace-level and per-shard changes, including shard namespace backfill,
// so apply gates see every change that guarantees a failed apply.
func TestPlanBlockedChanges(t *testing.T) {
	plan := &Plan{Namespaces: map[string]*NamespacePlanData{
		"testdb": {Tables: []TableChange{
			{Namespace: "testdb", Table: "users", Operation: "alter", ExecutionMode: "blocked", ModeReason: "requires copy-and-swap"},
			{Namespace: "testdb", Table: "orders", Operation: "alter"},
			{Namespace: "testdb", Table: "products", Operation: "alter", ExecutionMode: "direct"},
		}},
	}, Shards: []ShardPlan{{
		Namespace: "testdb",
		Shard:     "-80",
		Changes:   []TableChange{{Table: "accounts", Operation: "alter", ExecutionMode: "BLOCKED"}},
	}}}

	changes := plan.BlockedChanges()

	require.Len(t, changes, 2)
	assert.Equal(t, "users", changes[0].Table)
	assert.Equal(t, "requires copy-and-swap", changes[0].ModeReason)
	assert.Equal(t, "accounts", changes[1].Table)
	assert.Equal(t, "testdb", changes[1].Namespace)
}

// TestReleasesPausedRollout verifies the one-way release latch semantics: a
// pending or completed release request releases a paused rollout, while a
// failed release, any non-release operation, and a nil request do not (the
// rollout stays paused — fail-closed).
func TestReleasesPausedRollout(t *testing.T) {
	tests := []struct {
		name string
		req  *ApplyControlRequest
		want bool
	}{
		{name: "nil request", req: nil, want: false},
		{
			name: "pending release latches",
			req:  &ApplyControlRequest{Operation: ControlOperationRelease, Status: ControlRequestPending},
			want: true,
		},
		{
			name: "completed release latches",
			req:  &ApplyControlRequest{Operation: ControlOperationRelease, Status: ControlRequestCompleted},
			want: true,
		},
		{
			name: "failed release does not latch",
			req:  &ApplyControlRequest{Operation: ControlOperationRelease, Status: ControlRequestFailed},
			want: false,
		},
		{
			name: "pending start is not a release",
			req:  &ApplyControlRequest{Operation: ControlOperationStart, Status: ControlRequestPending},
			want: false,
		},
		{
			name: "completed stop is not a release",
			req:  &ApplyControlRequest{Operation: ControlOperationStop, Status: ControlRequestCompleted},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.req.ReleasesPausedRollout())
		})
	}
}

// EngineForType must route each database type to its engine, with Postgres
// mapping to the Postgres engine and unknown types defaulting to Spirit.
func TestEngineForType(t *testing.T) {
	tests := []struct {
		name   string
		dbType string
		want   string
	}{
		{name: "mysql", dbType: DatabaseTypeMySQL, want: EngineSpirit},
		{name: "vitess", dbType: DatabaseTypeVitess, want: EnginePlanetScale},
		{name: "strata", dbType: DatabaseTypeStrata, want: EngineStrata},
		{name: "postgres", dbType: DatabaseTypePostgres, want: EnginePostgres},
		{name: "empty defaults to spirit", dbType: "", want: EngineSpirit},
		{name: "unknown defaults to spirit", dbType: "unknown", want: EngineSpirit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, EngineForType(tc.dbType))
		})
	}
}

// The dispatch decision for a task-less claim rests entirely on these four
// predicates: a whole-deployment work operation for a VSchema-only plan is the
// one shape that legitimately resolves no tasks and may be dispatched, and every
// other task-less work operation must fail closed as an invalid or stale claim.
// Their boundaries — nil receivers, a nil plan, an absent versus empty artifact,
// a per-table operation key — are what separate the two outcomes.
func TestVSchemaPredicates(t *testing.T) {
	vschemaNamespace := func() *NamespacePlanData {
		return &NamespacePlanData{Artifacts: map[string]string{VSchemaArtifactName: `{"tables":{}}`}}
	}

	t.Run("ChangesVSchema", func(t *testing.T) {
		tests := []struct {
			name string
			ns   *NamespacePlanData
			want bool
		}{
			{"nil receiver", nil, false},
			{"no artifacts at all", &NamespacePlanData{}, false},
			{"nil artifact map", &NamespacePlanData{Artifacts: nil}, false},
			{"artifact absent", &NamespacePlanData{Artifacts: map[string]string{"other.json": "{}"}}, false},
			{"artifact present but empty", &NamespacePlanData{Artifacts: map[string]string{VSchemaArtifactName: ""}}, false},
			{"artifact present", vschemaNamespace(), true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, tt.want, tt.ns.ChangesVSchema())
			})
		}
	})

	t.Run("VSchemaNamespaces", func(t *testing.T) {
		assert.Nil(t, (*Plan)(nil).VSchemaNamespaces(), "a nil plan carries no namespaces")
		assert.Nil(t, (&Plan{}).VSchemaNamespaces(), "a plan with no namespaces carries none")

		plan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"zeta":       vschemaNamespace(),
			"alpha":      vschemaNamespace(),
			"tables":     {Tables: []TableChange{{Table: "orders"}}},
			"nil_entry":  nil,
			"empty_vsch": {Artifacts: map[string]string{VSchemaArtifactName: ""}},
		}}
		assert.Equal(t, []string{"alpha", "zeta"}, plan.VSchemaNamespaces(),
			"only namespaces carrying a VSchema artifact, in sorted order so dispatch is deterministic")
	})

	t.Run("IsVSchemaOnly", func(t *testing.T) {
		tests := []struct {
			name string
			plan *Plan
			want bool
		}{
			{"nil plan", nil, false},
			{"empty plan", &Plan{}, false},
			{
				name: "DDL only",
				plan: &Plan{Namespaces: map[string]*NamespacePlanData{
					"app": {Tables: []TableChange{{Table: "orders"}}},
				}},
				want: false,
			},
			{
				name: "VSchema only",
				plan: &Plan{Namespaces: map[string]*NamespacePlanData{"app": vschemaNamespace()}},
				want: true,
			},
			{
				name: "VSchema alongside DDL is not VSchema-only",
				plan: &Plan{Namespaces: map[string]*NamespacePlanData{
					"app":   vschemaNamespace(),
					"other": {Tables: []TableChange{{Table: "orders"}}},
				}},
				want: false,
			},
			{
				name: "DDL in the same namespace as the VSchema change",
				plan: &Plan{Namespaces: map[string]*NamespacePlanData{
					"app": {
						Tables:    []TableChange{{Table: "orders"}},
						Artifacts: map[string]string{VSchemaArtifactName: `{"tables":{}}`},
					},
				}},
				want: false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, tt.want, tt.plan.IsVSchemaOnly())
			})
		}
	})

	t.Run("IsTasklessVSchemaOnlyWork", func(t *testing.T) {
		vschemaOnlyPlan := &Plan{Namespaces: map[string]*NamespacePlanData{"app": vschemaNamespace()}}
		ddlPlan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"app": {Tables: []TableChange{{Table: "orders"}}},
		}}

		tests := []struct {
			name string
			op   *ApplyOperation
			plan *Plan
			want bool
		}{
			{"nil operation", nil, vschemaOnlyPlan, false},
			{"nil plan", &ApplyOperation{OperationKind: ApplyOperationKindWork}, nil, false},
			{
				name: "whole-deployment work on a VSchema-only plan",
				op:   &ApplyOperation{OperationKind: ApplyOperationKindWork},
				plan: vschemaOnlyPlan,
				want: true,
			},
			{
				name: "a keyed operation covers one table, so it is never the VSchema shape",
				op:   &ApplyOperation{OperationKind: ApplyOperationKindWork, OperationKey: "app.orders"},
				plan: vschemaOnlyPlan,
			},
			{
				name: "a finalizer is not work",
				op:   &ApplyOperation{OperationKind: ApplyOperationKindGroupFinalizer},
				plan: vschemaOnlyPlan,
			},
			{
				name: "work on a plan carrying DDL must fail closed rather than dispatch",
				op:   &ApplyOperation{OperationKind: ApplyOperationKindWork},
				plan: ddlPlan,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, tt.want, tt.op.IsTasklessVSchemaOnlyWork(tt.plan))
			})
		}
	})
}

// The generation manifest is the completion authority for a deployment-keyed
// apply: an operation key outside it must never attach, and the apply must not
// complete while a declared key has no attached row. An apply without a
// manifest keeps the attached-rows-only semantics.
func TestApplyGenerationManifestHelpers(t *testing.T) {
	t.Run("AllowsOperationKey", func(t *testing.T) {
		var noApply *Apply
		assert.True(t, noApply.AllowsOperationKey("commerce/-80/users"),
			"a missing apply row cannot refuse; the caller's own guards decide")

		noManifest := &Apply{}
		assert.True(t, noManifest.AllowsOperationKey("commerce/-80/users"),
			"an apply without a manifest accepts any key")

		apply := &Apply{ExpectedOperationKeys: []string{"commerce/-80/users", "commerce/80-/users"}}
		assert.True(t, apply.AllowsOperationKey("commerce/80-/users"))
		assert.False(t, apply.AllowsOperationKey("commerce/c0-/users"),
			"a key the completion gate never waits for must be refused")
	})

	t.Run("MissingExpectedOperationKeys", func(t *testing.T) {
		attached := func(keys ...string) []*ApplyOperation {
			ops := make([]*ApplyOperation, 0, len(keys))
			for _, key := range keys {
				ops = append(ops, &ApplyOperation{OperationKey: key})
			}
			return ops
		}

		noManifest := &Apply{}
		assert.Nil(t, noManifest.MissingExpectedOperationKeys(attached("commerce/-80/users")),
			"no manifest means nothing can be missing")

		apply := &Apply{ExpectedOperationKeys: []string{
			"commerce/-80/users", "commerce/80-/users", "commerce/group_finalizer",
		}}
		assert.Equal(t, []string{"commerce/80-/users", "commerce/group_finalizer"},
			apply.MissingExpectedOperationKeys(attached("commerce/-80/users")),
			"every declared key without an attached row is missing, finalizers included")
		assert.Nil(t,
			apply.MissingExpectedOperationKeys(attached(
				"commerce/-80/users", "commerce/80-/users", "commerce/group_finalizer")),
			"a fully attached manifest has no missing keys")
	})
}

// The grouping an apply runs under is read twice — once by the drive choosing
// how to execute, once by the engine predicting what that execution does to a
// copy already on the target — and the two are only consistent because they ask
// the same question here. Vitess covers a whole change in one deploy request;
// MySQL groups only when the caller asked to defer cutover so every table can
// swap together, and otherwise drives a table at a time.
func TestGroupsEngineExecution(t *testing.T) {
	cases := []struct {
		name         string
		databaseType string
		deferCutover bool
		grouped      bool
	}{
		{"vitess groups whether or not cutover is deferred", DatabaseTypeVitess, false, true},
		{"vitess groups with a deferred cutover too", DatabaseTypeVitess, true, true},
		{"mysql groups when the cutover is deferred", DatabaseTypeMySQL, true, true},
		{"mysql drives a table at a time by default", DatabaseTypeMySQL, false, false},
		{"postgres never groups", DatabaseTypePostgres, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.grouped, GroupsEngineExecution(tc.databaseType, tc.deferCutover))
		})
	}
}
