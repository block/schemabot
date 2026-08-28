package webhook

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// A sharded apply comment renders the attribution from the username, not the raw
// structured caller (which produced a malformed "@github:morgo@…#…" line).
func TestFormatApplyStatusComment_ShardedAttributionFromCaller(t *testing.T) {
	apply := &storage.Apply{
		ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Running,
		Caller: "github:morgo@block/example#11890",
	}
	op := &storage.ApplyOperation{ID: 1, ApplyID: 1, Deployment: "cake", OperationKey: "cdb_resolute_sharded/-40/mutes", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	oid := int64(1)
	tasks := []*storage.Task{{ID: 1, ApplyID: 1, ApplyOperationID: &oid, Namespace: "cdb_resolute_sharded", TableName: "mutes", Shard: "-40", DDL: "ALTER TABLE `mutes` ADD INDEX a"}}

	out := formatApplyStatusComment(apply, []*storage.ApplyOperation{op}, false, tasks, nil, nil, nil, "")

	assert.Contains(t, out, "by @morgo at", "attribution shows the clean username")
	assert.NotContains(t, out, "github:", "the raw structured caller is not rendered")
}

func TestParseShardOperationKey(t *testing.T) {
	ns, shard, table, ok := parseShardOperationKey("cdb_resolute_sharded/-40/mutes")
	require.True(t, ok)
	assert.Equal(t, "cdb_resolute_sharded", ns)
	assert.Equal(t, "-40", shard)
	assert.Equal(t, "mutes", table)

	for _, key := range []string{"", "cdb_resolute_sharded/group_finalizer", "deployment-only", "ns//table"} {
		_, _, _, ok := parseShardOperationKey(key)
		assert.False(t, ok, "key %q must not parse as a shard work key", key)
	}

	finalizerNS, ok := parseFinalizerOperationKey("cdb_resolute_sharded/group_finalizer")
	require.True(t, ok)
	assert.Equal(t, "cdb_resolute_sharded", finalizerNS)
	for _, key := range []string{"cdb_resolute_sharded/-40/mutes", "group_finalizer", ""} {
		_, ok := parseFinalizerOperationKey(key)
		assert.False(t, ok, "key %q must not parse as a finalizer key", key)
	}
}

func TestIsShardedApply(t *testing.T) {
	shardOp := func(shard string) *storage.ApplyOperation {
		return &storage.ApplyOperation{Deployment: "cake", OperationKey: "ks/" + shard + "/mutes"}
	}
	finalizer := &storage.ApplyOperation{Deployment: "cake", OperationKey: "ks/group_finalizer"}

	assert.True(t, isShardedApply([]*storage.ApplyOperation{shardOp("-40"), shardOp("80-"), finalizer}),
		"shard work + finalizer in one deployment is sharded")
	assert.False(t, isShardedApply([]*storage.ApplyOperation{finalizer}), "a finalizer alone has no shard work")
	assert.False(t, isShardedApply([]*storage.ApplyOperation{
		{Deployment: "cake", OperationKey: ""}, {Deployment: "eu", OperationKey: ""},
	}), "empty keys are a non-sharded multi-deployment apply")
	assert.False(t, isShardedApply([]*storage.ApplyOperation{
		{Deployment: "cake", OperationKey: "ks/-40/mutes"}, {Deployment: "eu", OperationKey: "ks/80-/mutes"},
	}), "shards spanning deployments fall back to the deployment layout")
	assert.True(t, isShardedApply([]*storage.ApplyOperation{
		{Deployment: "cake", OperationKey: "ks1/-40/mutes"}, {Deployment: "cake", OperationKey: "ks2/-40/mutes"},
	}), "shard work across multiple keyspaces in one deployment is sharded")
}

// The failed sharded apply must render the shard-unit layout AND surface the
// failed shard's error — the bug was that the deployment-keyed layout collided
// per-shard details and dropped the error.
func TestFormatApplyStatusComment_ShardedFailedSurfacesError(t *testing.T) {
	const failErr = "resolve shard primary for `-40`: context deadline exceeded"
	started := time.Unix(1700000000, 0).UTC()
	apply := &storage.Apply{
		ApplyIdentifier: "apply-f5701ad9", Database: "cdb_resolute", Environment: "staging",
		State: state.Apply.Failed, Caller: "morgo", StartedAt: &started,
	}
	op := func(id int64, shard, opState, errMsg string) *storage.ApplyOperation {
		return &storage.ApplyOperation{
			ID: id, ApplyID: 1, Deployment: "cake",
			OperationKey: "cdb_resolute_sharded/" + shard + "/mutes",
			State:        opState, ErrorMessage: errMsg,
			CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt,
		}
	}
	// Resolved order: the failed shard first, so the rest derive as halted by it.
	ops := []*storage.ApplyOperation{
		op(1, "-40", state.ApplyOperation.Failed, failErr),
		op(2, "40-80", state.ApplyOperation.Pending, ""),
		op(3, "80-c0", state.ApplyOperation.Pending, ""),
		op(4, "c0-", state.ApplyOperation.Pending, ""),
	}
	task := func(id int64, opID int64, shard string) *storage.Task {
		oid := opID
		return &storage.Task{
			ID: id, ApplyID: 1, ApplyOperationID: &oid, Shard: shard,
			Namespace: "cdb_resolute_sharded", TableName: "mutes",
			DDL: "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`);",
		}
	}
	tasks := []*storage.Task{task(1, 1, "-40"), task(2, 2, "40-80"), task(3, 3, "80-c0"), task(4, 4, "c0-")}

	out := formatApplyStatusComment(apply, ops, false, tasks, nil, nil, nil, "")

	assert.Contains(t, out, "## Schema Change Status", "uses the stable in-place status headline")
	assert.Contains(t, out, "**Shards**:", "counts shards, not deployments")
	assert.NotContains(t, out, "**Deployments**:", "must not use the deployment-unit layout")
	assert.Contains(t, out, failErr, "the failed shard's error is surfaced (the bug fix)")
	assert.Contains(t, out, "First failure:", "the failure is lifted to the top")
	for _, shard := range []string{"-40", "40-80", "80-c0", "c0-"} {
		assert.Contains(t, out, "`"+shard+"`", "shard %s is shown", shard)
	}
}

// A remote failure records the error on the operation's task, and the operator
// may not stamp it onto the operation row. The apply comment must still surface
// it (falling back to the task error) rather than going silent — the gap that
// forced digging through Datadog.
func TestFormatApplyStatusComment_ShardedFailureFallsBackToTaskError(t *testing.T) {
	const gotZero = "strata work operation expected exactly one target shard, got 0"
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Failed}
	op := &storage.ApplyOperation{
		ID: 1, ApplyID: 1, Deployment: "cake", OperationKey: "cdb_resolute_sharded/-40/mutes",
		State: state.ApplyOperation.Failed, ErrorMessage: "", // operation row carries no error
		CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt,
	}
	oid := int64(1)
	tasks := []*storage.Task{{ID: 1, ApplyID: 1, ApplyOperationID: &oid, Namespace: "cdb_resolute_sharded", TableName: "mutes", Shard: "-40", DDL: "ALTER ...", ErrorMessage: gotZero}}

	out := formatApplyStatusComment(apply, []*storage.ApplyOperation{op}, false, tasks, nil, nil, nil, "")

	assert.Contains(t, out, gotZero, "the task error is surfaced when the operation row has none")
}

// A divergent sharded apply groups shards by change signature and keeps each
// table's DDL once; the keyspace and cells come from the operation keys/tasks.
func TestBuildShardedApplyData_DivergentGroupsByTable(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Running}
	mk := func(id int64, key string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: 1, Deployment: "cake", OperationKey: key, State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	}
	ops := []*storage.ApplyOperation{
		mk(1, "ks/-40/mutes"),
		mk(2, "ks/40-80/mutes"),
		mk(3, "ks/40-80/blocks"), // 40-80 diverges: it also changes blocks
	}
	tk := func(id, opID int64, table string) *storage.Task {
		oid := opID
		return &storage.Task{ID: id, ApplyID: 1, ApplyOperationID: &oid, Namespace: "ks", TableName: table, DDL: "ALTER `" + table + "`"}
	}
	tasks := []*storage.Task{tk(1, 1, "mutes"), tk(2, 2, "mutes"), tk(3, 3, "blocks")}

	data := buildShardedApplyData(apply, ops, false, tasks, nil, "")

	require.Len(t, data.Keyspaces, 1)
	ks := data.Keyspaces[0]
	assert.Equal(t, "ks", ks.Keyspace)
	require.Len(t, ks.Cells, 3)
	require.Len(t, ks.Shards, 2, "two distinct shards, each shown once")
	assert.Equal(t, "-40", ks.Shards[0].Shard)
	assert.Equal(t, "40-80", ks.Shards[1].Shard)
}

// Defensive: in practice a (shard, table) operation has a single task — multiple
// statements for one table are combined into one ALTER upstream — but if more
// than one task ever shows up, every non-empty DDL is joined in deterministic id
// order rather than dropping all but the first.
func TestBuildShardedApplyData_JoinsMultiTaskDDL(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Running}
	op := &storage.ApplyOperation{ID: 1, ApplyID: 1, Deployment: "cake", OperationKey: "ks/-40/mutes", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	oid := int64(1)
	tasks := []*storage.Task{
		{ID: 1, ApplyID: 1, ApplyOperationID: &oid, Namespace: "ks", TableName: "mutes", DDL: "ALTER TABLE `mutes` ADD INDEX a"},
		{ID: 2, ApplyID: 1, ApplyOperationID: &oid, Namespace: "ks", TableName: "mutes", DDL: ""}, // empty is skipped
		{ID: 3, ApplyID: 1, ApplyOperationID: &oid, Namespace: "ks", TableName: "mutes", DDL: "ALTER TABLE `mutes` ADD INDEX b"},
	}

	data := buildShardedApplyData(apply, []*storage.ApplyOperation{op}, false, tasks, nil, "")

	require.Len(t, data.Keyspaces, 1)
	require.Len(t, data.Keyspaces[0].Cells, 1)
	assert.Equal(t, "ALTER TABLE `mutes` ADD INDEX a\nALTER TABLE `mutes` ADD INDEX b", data.Keyspaces[0].Cells[0].DDL,
		"all non-empty task DDLs are joined in order")
}

// A sharded apply's finalizer operations are its keyspace VSchema changes:
// each one surfaces as a VSchema change whose keyspace comes from the
// operation key, whose display status tracks the operation state, and whose
// diff comes from the stored plan's per-namespace diffs, so the PR comment
// shows the finalizer's progress — and the approved change — alongside the
// shard rollout.
func TestBuildShardedApplyData_FinalizerBecomesVSchemaChange(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Running}
	mk := func(id int64, key, opState string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: 1, Deployment: "cake", OperationKey: key, State: opState, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	}
	ops := []*storage.ApplyOperation{
		mk(1, "ks/-40/mutes", state.ApplyOperation.Completed),
		mk(2, "ks/80-/mutes", state.ApplyOperation.Completed),
		mk(3, "ks/group_finalizer", state.ApplyOperation.Running),
	}

	data := buildShardedApplyData(apply, ops, false, nil, map[string]string{"ks": "+ vindex hash"}, "")

	require.Len(t, data.VSchemaChanges, 1)
	assert.Equal(t, "ks", data.VSchemaChanges[0].Namespace)
	assert.Equal(t, "applying", data.VSchemaChanges[0].Status)
	assert.Equal(t, "+ vindex hash", data.VSchemaChanges[0].Diff, "the stored plan's diff rides on the keyspace's entry")
	require.Len(t, data.Keyspaces, 1)
	require.Len(t, data.Keyspaces[0].Shards, 2, "the finalizer is not a shard row")

	data = buildShardedApplyData(apply, ops, false, nil, nil, "")
	require.Len(t, data.VSchemaChanges, 1)
	assert.Empty(t, data.VSchemaChanges[0].Diff, "a stored plan without diffs renders the status-only entry")
}

// A finalizer whose rollout ended without running it must not read as
// pending, whichever record says so: a cancelled/reverted operation row
// (written by the cancel path or mirrored by the stranded-operation reaper)
// reads as cancelled, and so does a row still pending under a parent whose
// verdict is final — a halted rollout terminalizes the apply immediately,
// while the reaper only settles the stranded row minutes after the summary
// posted. Stopped — on the operation or the parent — reads as stopped: a
// stopped apply is resumable, so its finalizer may yet run.
func TestVSchemaStatusForOperationState_TerminalInertStates(t *testing.T) {
	running := state.Apply.Running
	assert.Equal(t, "cancelled", vschemaStatusForOperationState(running, state.ApplyOperation.Cancelled))
	assert.Equal(t, "cancelled", vschemaStatusForOperationState(running, state.ApplyOperation.Reverted))
	assert.Equal(t, "stopped", vschemaStatusForOperationState(running, state.ApplyOperation.Stopped))
	assert.Equal(t, "", vschemaStatusForOperationState(running, state.ApplyOperation.Pending),
		"a pending finalizer under a live apply still reads as pending")

	pending := state.ApplyOperation.Pending
	assert.Equal(t, "cancelled", vschemaStatusForOperationState(state.Apply.Failed, pending),
		"a halted rollout's terminal summary must not promise VSchema work no claim arm will run")
	assert.Equal(t, "cancelled", vschemaStatusForOperationState(state.Apply.Cancelled, pending))
	assert.Equal(t, "stopped", vschemaStatusForOperationState(state.Apply.Stopped, pending),
		"a stopped apply's pending finalizer may yet run on resume")
	assert.Equal(t, "failed", vschemaStatusForOperationState(state.Apply.Failed, state.ApplyOperation.Failed),
		"the operation's own failure outranks the parent verdict")
}

// stubPlanStorage serves one stored plan (or a load error) for resolver tests.
// Every store other than Plans() panics if touched, so a test that must not
// read storage can pass a zero-valued stub and rely on the panic.
type stubPlanStorage struct {
	storage.Storage
	plan *storage.Plan
	err  error
}

func (s *stubPlanStorage) Plans() storage.PlanStore {
	return &stubPlanStore{plan: s.plan, err: s.err}
}

type stubPlanStore struct {
	storage.PlanStore
	plan *storage.Plan
	err  error
}

func (s *stubPlanStore) GetByID(_ context.Context, _ int64) (*storage.Plan, error) {
	return s.plan, s.err
}

// The sharded comment's VSchema diffs come from the stored plan: the resolver
// reads each namespace's persisted diff so the comment shows the change the
// operator approved at plan time. Degraded storage (a load error or a missing
// plan row) and older stored plans without diffs must render the comment
// without diffs rather than blocking it, and an apply with no finalizer
// operation must not read storage at all.
func TestResolveShardedVSchemaDiffs(t *testing.T) {
	shardOp := &storage.ApplyOperation{OperationKey: "ks/-40/mutes"}
	finalizerOp := &storage.ApplyOperation{OperationKey: "ks/group_finalizer"}
	apply := &storage.Apply{ApplyIdentifier: "apply-x", PlanID: 7}
	ops := []*storage.ApplyOperation{shardOp, finalizerOp}

	plan := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
		"ks":    {Metadata: map[string]string{storage.PlanMetadataVSchemaChanged: "true", storage.PlanMetadataVSchemaDiff: "+ vindex hash"}},
		"other": {Metadata: map[string]string{storage.PlanMetadataVSchemaChanged: "true"}},
	}}
	diffs := resolveShardedVSchemaDiffs(t.Context(), &stubPlanStorage{plan: plan}, apply, ops)
	assert.Equal(t, map[string]string{"ks": "+ vindex hash"}, diffs,
		"only namespaces with a persisted diff contribute")

	assert.Nil(t, resolveShardedVSchemaDiffs(t.Context(), &stubPlanStorage{err: errors.New("storage down")}, apply, ops),
		"a plan load failure degrades to no diffs")
	assert.Nil(t, resolveShardedVSchemaDiffs(t.Context(), &stubPlanStorage{}, apply, ops),
		"a missing plan row degrades to no diffs")
	assert.Nil(t, resolveShardedVSchemaDiffs(t.Context(), &stubPlanStorage{plan: &storage.Plan{}}, apply, ops),
		"a stored plan without diff metadata degrades to no diffs")

	// No finalizer operation → nothing to attach a diff to; the nil Storage
	// would panic on any read, proving the resolver does not touch storage.
	assert.Nil(t, resolveShardedVSchemaDiffs(t.Context(), nil, apply, []*storage.ApplyOperation{shardOp}))

	// A shape that is not the sharded layout — here a two-deployment apply —
	// discards the diffs downstream, so the resolver must not pay the
	// stored-plan read for it: the nil Storage would panic on any read.
	multiDeployment := []*storage.ApplyOperation{
		{OperationKey: "ks/-40/mutes", Deployment: "cake"},
		{OperationKey: "ks/group_finalizer", Deployment: "ski"},
	}
	assert.Nil(t, resolveShardedVSchemaDiffs(t.Context(), nil, apply, multiDeployment))
}

// The stored-plan diff must reach a real PR comment through the production
// observer path: the observer resolves the diffs from storage and threads
// them into the comment body, so a sharded apply's progress comment shows the
// change the operator approved at plan time.
func TestCommentObserverResolvedDiffReachesShardedComment(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Running, PlanID: 7}
	mk := func(id int64, key string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: 1, Deployment: "cake", OperationKey: key, State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	}
	ops := []*storage.ApplyOperation{mk(1, "ks/-40/mutes"), mk(2, "ks/80-/mutes"), mk(3, "ks/group_finalizer")}
	plan := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
		"ks": {Metadata: map[string]string{storage.PlanMetadataVSchemaChanged: "true", storage.PlanMetadataVSchemaDiff: "+ vindex hash"}},
	}}

	o := &CommentObserver{stor: &stubPlanStorage{plan: plan}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	body := formatApplyStatusComment(apply, ops, false, nil, nil, nil, o.resolveVSchemaDiffs(apply, ops), "")

	assert.Contains(t, body, "### VSchema")
	assert.Contains(t, body, "```diff\n+ vindex hash\n```",
		"the stored plan's diff renders in the observer-built comment body")
}

// A finalizer failure is operation-scoped: it does not write the parent
// apply's error. When the apply row carries no cause of its own, the failed
// finalizer's error stands in so the failure callout still names the cause —
// but an apply-level error, when present, wins.
func TestBuildShardedApplyData_FailedFinalizerErrorFallsBack(t *testing.T) {
	mk := func(id int64, key, opState, errMsg string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: 1, Deployment: "cake", OperationKey: key, State: opState, ErrorMessage: errMsg, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	}
	ops := []*storage.ApplyOperation{
		mk(1, "ks/-40/mutes", state.ApplyOperation.Completed, ""),
		mk(2, "ks/group_finalizer", state.ApplyOperation.Failed, "finalize vschema: apply vschema to keyspace: context deadline exceeded"),
	}

	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Failed}
	data := buildShardedApplyData(apply, ops, false, nil, nil, "")
	assert.Equal(t, "finalize vschema: apply vschema to keyspace: context deadline exceeded", data.ErrorMessage,
		"the failed finalizer's error stands in when the apply row carries none")
	require.Len(t, data.VSchemaChanges, 1)
	assert.Equal(t, "failed", data.VSchemaChanges[0].Status)

	apply.ErrorMessage = "apply-level cause"
	data = buildShardedApplyData(apply, ops, false, nil, nil, "")
	assert.Equal(t, "apply-level cause", data.ErrorMessage, "the apply row's own error wins when present")
}

// An auto-retrying finalizer carries an operator-facing error just like a
// terminal failure: it renders as failed with its error surfaced, so the
// operator sees the cause while the retry is still in flight.
func TestBuildShardedApplyData_RetryingFinalizerSurfacesError(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, ApplyID: 1, Deployment: "cake", OperationKey: "ks/-40/mutes", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{ID: 2, ApplyID: 1, Deployment: "cake", OperationKey: "ks/group_finalizer", State: state.ApplyOperation.FailedRetryable, ErrorMessage: "finalize vschema: transient topo error", CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
	}
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Running}

	data := buildShardedApplyData(apply, ops, false, nil, nil, "")
	require.Len(t, data.VSchemaChanges, 1)
	assert.Equal(t, "failed", data.VSchemaChanges[0].Status)
	assert.Equal(t, "finalize vschema: transient topo error", data.ErrorMessage)
}

// A finalizer that failed, retried, and completed keeps its last error on the
// operation row; a completed finalizer's stale error must not become the
// apply's failure callout.
func TestBuildShardedApplyData_CompletedFinalizerStaleErrorIgnored(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, ApplyID: 1, Deployment: "cake", OperationKey: "ks/-40/mutes", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{ID: 2, ApplyID: 1, Deployment: "cake", OperationKey: "ks/group_finalizer", State: state.ApplyOperation.Completed, ErrorMessage: "finalize vschema: transient topo error", CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
	}
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Completed}

	data := buildShardedApplyData(apply, ops, false, nil, nil, "")
	require.Len(t, data.VSchemaChanges, 1)
	assert.Equal(t, "applied", data.VSchemaChanges[0].Status)
	assert.Empty(t, data.ErrorMessage, "a completed finalizer's stale error is not a failure cause")
}

// When more than one finalizer failed, the surfaced cause is the first failed
// finalizer's error in operation order — deterministic across comment edits,
// never flipping between causes from one render to the next.
func TestBuildShardedApplyData_FirstFailedFinalizerErrorWins(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, ApplyID: 1, Deployment: "cake", OperationKey: "ks_a/group_finalizer", State: state.ApplyOperation.Failed, ErrorMessage: "finalize ks_a: first cause", CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
		{ID: 2, ApplyID: 1, Deployment: "cake", OperationKey: "ks_b/group_finalizer", State: state.ApplyOperation.Failed, ErrorMessage: "finalize ks_b: second cause", CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
	}
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Failed}

	data := buildShardedApplyData(apply, ops, false, nil, nil, "")
	assert.Equal(t, "finalize ks_a: first cause", data.ErrorMessage)
}

// A Strata apply that fans out across several keyspaces of one deployment —
// unsharded siblings contribute a single "-" shard each — renders the sharded
// layout with one section per keyspace, so the operator reads the rollout by
// keyspace rather than a flat per-operation list keyed by the deployment name.
func TestFormatApplyStatusComment_MultiKeyspaceRendersKeyspaceSections(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_contacts", Environment: "staging", State: state.Apply.Running, Caller: "morgo"}
	mk := func(id int64, key, opState string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: 1, Deployment: "cake", OperationKey: key, State: opState, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	}
	ops := []*storage.ApplyOperation{
		mk(1, "contacts/-/entries", state.ApplyOperation.Completed),
		mk(2, "contacts_lookup/-/entries_lookup", state.ApplyOperation.Running),
		mk(3, "contacts_sharded/-40/entries_index", state.ApplyOperation.Pending),
		mk(4, "contacts_sharded/40-/entries_index", state.ApplyOperation.Pending),
		mk(5, "contacts_sharded/group_finalizer", state.ApplyOperation.Pending),
	}

	out := formatApplyStatusComment(apply, ops, false, nil, nil, nil, nil, "")

	assert.NotContains(t, out, "**Deployments**:", "must not fall back to the deployment-unit layout")
	assert.Contains(t, out, "#### Keyspace `contacts`")
	assert.Contains(t, out, "#### Keyspace `contacts_lookup`")
	assert.Contains(t, out, "#### Keyspace `contacts_sharded`")
	assert.Contains(t, out, "**Shards**:", "the histogram spans every keyspace's shards")
	assert.Contains(t, out, "**Status**: In Progress — 1 of 4 changes applied",
		"three table units plus the VSchema update make four changes, one landed")
	assert.Contains(t, out, "**`entries`**: ✅ Complete", "each keyspace section renders its tables")
	assert.Contains(t, out, "**`entries_lookup`**: 🔄 Row copy in progress")
	assert.Contains(t, out, "**`entries_index`**: ⏳ Queued")
	assert.NotContains(t, out, "| Shard | Status |", "healthy uniform keyspaces render no per-shard tables")
	assert.Contains(t, out, "### VSchema", "the finalizer renders in the VSchema section, not as a shard")
}

// When the apply spans keyspaces, a shard's ordering label names its blocker
// with the keyspace-qualified shard — every unsharded keyspace's shard is "-",
// so a bare name would be ambiguous — while the status row itself keeps the
// plain shard name under its keyspace heading.
func TestBuildShardedApplyData_MultiKeyspaceQualifiesOrderingLabels(t *testing.T) {
	mk := func(id int64, key, opState, errMsg string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: 1, Deployment: "cake", OperationKey: key, State: opState, ErrorMessage: errMsg, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	}
	ops := []*storage.ApplyOperation{
		mk(1, "contacts/-/entries", state.ApplyOperation.Failed, "boom"),
		mk(2, "contacts_lookup/-/entries_lookup", state.ApplyOperation.Pending, ""),
	}
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_contacts", Environment: "staging", State: state.Apply.Failed}

	data := buildShardedApplyData(apply, ops, false, nil, nil, "")

	require.Len(t, data.Keyspaces, 2)
	require.Len(t, data.Keyspaces[1].Shards, 1)
	halted := data.Keyspaces[1].Shards[0]
	assert.Equal(t, "-", halted.Shard, "the status row keeps the plain shard name")
	assert.Contains(t, halted.Label, "contacts/-", "the blocker is named with its keyspace-qualified shard")
}

// A terminal sharded apply's summary comment renders the verdict-titled
// shard-unit summary — not the status snapshot the progress comment shows — so
// the last word on the PR states the outcome, with every shard's final state.
func TestFormatApplySummaryComment_ShardedRendersVerdict(t *testing.T) {
	started := time.Unix(1700000000, 0).UTC()
	completed := started.Add(28 * time.Minute)
	apply := &storage.Apply{
		ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging",
		State: state.Apply.Completed, Caller: "morgo", StartedAt: &started, CompletedAt: &completed,
	}
	op := func(id int64, shard string) *storage.ApplyOperation {
		return &storage.ApplyOperation{
			ID: id, ApplyID: 1, Deployment: "cake",
			OperationKey:  "cdb_resolute_sharded/" + shard + "/mutes",
			State:         state.ApplyOperation.Completed,
			CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt,
		}
	}
	ops := []*storage.ApplyOperation{op(1, "-40"), op(2, "80-")}
	task := func(id, opID int64, shard string) *storage.Task {
		oid := opID
		return &storage.Task{
			ID: id, ApplyID: 1, ApplyOperationID: &oid, Shard: shard,
			Namespace: "cdb_resolute_sharded", TableName: "mutes",
			DDL: "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`);",
		}
	}
	tasks := []*storage.Task{task(1, 1, "-40"), task(2, 2, "80-")}

	out := formatApplySummaryComment(apply, ops, false, tasks, nil, nil, nil, "")

	assert.Contains(t, out, "## ✅ Schema Change Applied — Staging")
	assert.NotContains(t, out, "Schema Change Status", "the summary is a verdict, not a status snapshot")
	assert.Contains(t, out, "**Shards**: 2 completed")
	assert.Contains(t, out, "**Duration**: 28m")
	assert.Contains(t, out, "**`mutes`**: ✅ Complete (2 shards)")
	assert.NotContains(t, out, "| Shard | Status |", "a fully completed keyspace renders no per-shard table")
	assert.NotContains(t, out, "**Deployments**:", "must not use the deployment-unit layout")
}

// A completed sharded apply with a finalizer operation renders the applied
// VSchema change in the terminal summary, end to end from the operation rows.
func TestFormatApplySummaryComment_ShardedVSchemaSection(t *testing.T) {
	apply := &storage.Apply{
		ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging",
		State: state.Apply.Completed, Caller: "morgo",
	}
	mk := func(id int64, key string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: 1, Deployment: "cake", OperationKey: key, State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	}
	ops := []*storage.ApplyOperation{
		mk(1, "cdb_resolute_sharded/-40/mutes"),
		mk(2, "cdb_resolute_sharded/80-/mutes"),
		mk(3, "cdb_resolute_sharded/group_finalizer"),
	}

	out := formatApplySummaryComment(apply, ops, false, nil, nil, nil, nil, "")

	assert.Contains(t, out, "## ✅ Schema Change Applied — Staging")
	assert.Contains(t, out, "### VSchema")
	assert.Contains(t, out, "**`cdb_resolute_sharded`**: Applied")
	assert.Contains(t, out, "your schema changes are live!", "a table change plus a VSchema update reads as plural")
	assert.Contains(t, out, "**Shards**: 2 completed", "the finalizer is not counted as a shard")
}

// A sharded apply that fails outside shard work (e.g. its finalizer operation)
// records the cause on the apply row, not on any shard operation. The terminal
// summary must carry that apply-level error through to the failure callout so
// the failed verdict names its cause.
func TestFormatApplySummaryComment_ShardedApplyLevelErrorSurfaced(t *testing.T) {
	apply := &storage.Apply{
		ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging",
		State: state.Apply.Failed, Caller: "morgo",
		ErrorMessage: "finalize vschema: apply vschema to keyspace: context deadline exceeded",
	}
	op := func(id int64, shard string) *storage.ApplyOperation {
		return &storage.ApplyOperation{
			ID: id, ApplyID: 1, Deployment: "cake",
			OperationKey:  "cdb_resolute_sharded/" + shard + "/mutes",
			State:         state.ApplyOperation.Completed,
			CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt,
		}
	}
	ops := []*storage.ApplyOperation{op(1, "-40"), op(2, "80-")}

	out := formatApplySummaryComment(apply, ops, false, nil, nil, nil, nil, "")

	assert.Contains(t, out, "## ❌ Schema Change Failed — Staging")
	assert.Contains(t, out, "> ❌ **Failure:** finalize vschema: apply vschema to keyspace: context deadline exceeded",
		"the apply row's error reaches the callout when no shard carries the failure")
	assert.NotContains(t, out, "First failure:", "no shard failed, so there is no shard failure callout")
}

// The keyspace section's table rollup is derived from the tasks: each shard's
// entry carries its task's live state and copy percent, and the table's
// aggregate is its most attention-worthy shard state — so a table with one
// copying shard reads as copying even while the other shards are done or
// queued. An operation whose task has not been created yet contributes its
// operation state instead, so early dispatch waves still render.
func TestBuildShardedApplyData_TableRollupFromTasks(t *testing.T) {
	mk := func(id int64, key, opState string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: 1, Deployment: "cake", OperationKey: key, State: opState, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	}
	ops := []*storage.ApplyOperation{
		mk(1, "cdb_resolute_sharded/-40/mutes", state.ApplyOperation.Completed),
		mk(2, "cdb_resolute_sharded/40-80/mutes", state.ApplyOperation.Running),
		mk(3, "cdb_resolute_sharded/80-/mutes", state.ApplyOperation.Pending),
	}
	task := func(id, opID int64, shard, taskState string, percent int, copied, total int64, eta int) *storage.Task {
		oid := opID
		return &storage.Task{
			ID: id, ApplyID: 1, ApplyOperationID: &oid, Shard: shard,
			Namespace: "cdb_resolute_sharded", TableName: "mutes",
			State: taskState, ProgressPercent: percent,
			RowsCopied: copied, RowsTotal: total, ETASeconds: eta,
		}
	}
	tasks := []*storage.Task{
		task(1, 1, "-40", state.Task.Completed, 100, 500000, 500000, 0),
		task(2, 2, "40-80", state.Task.Running, 37, 185000, 500000, 240),
		// The 80- operation has no task yet: dispatch creates tasks when the
		// shard's wave starts, so its operation state stands in.
	}
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_resolute", Environment: "staging", State: state.Apply.Running}

	data := buildShardedApplyData(apply, ops, false, tasks, nil, "")

	require.Len(t, data.Keyspaces, 1)
	require.Len(t, data.Keyspaces[0].Tables, 1)
	table := data.Keyspaces[0].Tables[0]
	assert.Equal(t, "mutes", table.Table)
	assert.Equal(t, state.Task.Running, table.Status, "the copying shard is the most attention-worthy")
	require.Len(t, table.Shards, 3)
	assert.Equal(t, templates.ShardProgressData{Shard: "-40", Status: state.Task.Completed, PercentComplete: 100}, table.Shards[0])
	assert.Equal(t, templates.ShardProgressData{Shard: "40-80", Status: state.Task.Running, PercentComplete: 37}, table.Shards[1])
	assert.Equal(t, templates.ShardProgressData{Shard: "80-", Status: state.ApplyOperation.Pending, PercentComplete: 0}, table.Shards[2],
		"an operation without a task contributes its operation state")
	assert.Equal(t, int64(685000), table.RowsCopied, "rows sum across the shards that have reported")
	assert.Equal(t, int64(1000000), table.RowsTotal, "the taskless shard contributes no rows yet")
	assert.Equal(t, int64(240), table.ETASeconds, "the ETA is the slowest shard's")
}

// A shard whose table failed makes the whole table read failed, and each
// keyspace's tables keep resolved order even when their operations interleave
// with another keyspace's.
func TestBuildShardedApplyData_TableAggregateAndOrder(t *testing.T) {
	mk := func(id int64, key, opState string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: 1, Deployment: "cake", OperationKey: key, State: opState, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	}
	ops := []*storage.ApplyOperation{
		mk(1, "contacts_sharded/-40/entries", state.ApplyOperation.Completed),
		mk(2, "contacts/-/aliases", state.ApplyOperation.Pending),
		mk(3, "contacts_sharded/-40/blocks", state.ApplyOperation.Completed),
		mk(4, "contacts_sharded/40-/entries", state.ApplyOperation.Failed),
		mk(5, "contacts_sharded/40-/blocks", state.ApplyOperation.Pending),
	}
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_contacts", Environment: "staging", State: state.Apply.Failed}

	data := buildShardedApplyData(apply, ops, false, nil, nil, "")

	require.Len(t, data.Keyspaces, 2)
	require.Len(t, data.Keyspaces[0].Tables, 2)
	assert.Equal(t, "entries", data.Keyspaces[0].Tables[0].Table, "tables keep resolved order within their keyspace")
	assert.Equal(t, state.Task.Failed, data.Keyspaces[0].Tables[0].Status, "one failed shard makes the table read failed")
	assert.Equal(t, "blocks", data.Keyspaces[0].Tables[1].Table)
	assert.Equal(t, state.Task.Pending, data.Keyspaces[0].Tables[1].Status, "a queued shard outranks completed siblings")
	require.Len(t, data.Keyspaces[1].Tables, 1)
	assert.Equal(t, "aliases", data.Keyspaces[1].Tables[0].Table)
}

// Under wave dispatch, landed shards hold in their revert window while later
// waves are still queued. The table aggregate surfaces the queued work — the
// table still has a whole copy ahead of it — not the landed shards' hold
// state, so a mid-rollout table never reads as complete.
func TestBuildShardedApplyData_PendingOutranksRevertWindow(t *testing.T) {
	mk := func(id int64, key, opState string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: 1, Deployment: "cake", OperationKey: key, State: opState, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt}
	}
	ops := []*storage.ApplyOperation{
		mk(1, "contacts_sharded/-40/entries", state.ApplyOperation.RevertWindow),
		mk(2, "contacts_sharded/40-/entries", state.ApplyOperation.Pending),
	}
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "cdb_contacts", Environment: "staging", State: state.Apply.Running}

	data := buildShardedApplyData(apply, ops, false, nil, nil, "")

	require.Len(t, data.Keyspaces, 1)
	require.Len(t, data.Keyspaces[0].Tables, 1)
	assert.Equal(t, state.Task.Pending, data.Keyspaces[0].Tables[0].Status,
		"an undispatched shard outranks a sibling holding in its revert window")
}
