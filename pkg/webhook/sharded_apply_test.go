package webhook

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
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

	out := formatApplyStatusComment(apply, []*storage.ApplyOperation{op}, false, tasks, nil, nil, "")

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
	assert.False(t, isShardedApply([]*storage.ApplyOperation{
		{Deployment: "cake", OperationKey: "ks1/-40/mutes"}, {Deployment: "cake", OperationKey: "ks2/-40/mutes"},
	}), "shard work across multiple keyspaces falls back rather than mislabelling one keyspace")
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

	out := formatApplyStatusComment(apply, ops, false, tasks, nil, nil, "")

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

	out := formatApplyStatusComment(apply, []*storage.ApplyOperation{op}, false, tasks, nil, nil, "")

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

	data := buildShardedApplyData(apply, ops, false, tasks, "")

	assert.Equal(t, "ks", data.Keyspace)
	require.Len(t, data.Cells, 3)
	require.Len(t, data.Shards, 2, "two distinct shards, each shown once")
	assert.Equal(t, "-40", data.Shards[0].Shard)
	assert.Equal(t, "40-80", data.Shards[1].Shard)
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

	data := buildShardedApplyData(apply, []*storage.ApplyOperation{op}, false, tasks, "")

	require.Len(t, data.Cells, 1)
	assert.Equal(t, "ALTER TABLE `mutes` ADD INDEX a\nALTER TABLE `mutes` ADD INDEX b", data.Cells[0].DDL,
		"all non-empty task DDLs are joined in order")
}

// A sharded apply's finalizer operations are its keyspace VSchema changes:
// each one surfaces as a VSchema change whose keyspace comes from the
// operation key and whose display status tracks the operation state, so the
// PR comment shows the finalizer's progress alongside the shard rollout.
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

	data := buildShardedApplyData(apply, ops, false, nil, "")

	require.Len(t, data.VSchemaChanges, 1)
	assert.Equal(t, "ks", data.VSchemaChanges[0].Namespace)
	assert.Equal(t, "applying", data.VSchemaChanges[0].Status)
	assert.Empty(t, data.VSchemaChanges[0].Diff, "the rendered VSchema diff is not persisted, so sharded applies carry none")
	require.Len(t, data.Shards, 2, "the finalizer is not a shard row")
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
	data := buildShardedApplyData(apply, ops, false, nil, "")
	assert.Equal(t, "finalize vschema: apply vschema to keyspace: context deadline exceeded", data.ErrorMessage,
		"the failed finalizer's error stands in when the apply row carries none")
	require.Len(t, data.VSchemaChanges, 1)
	assert.Equal(t, "failed", data.VSchemaChanges[0].Status)

	apply.ErrorMessage = "apply-level cause"
	data = buildShardedApplyData(apply, ops, false, nil, "")
	assert.Equal(t, "apply-level cause", data.ErrorMessage, "the apply row's own error wins when present")
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

	out := formatApplySummaryComment(apply, ops, false, tasks, nil, nil, "")

	assert.Contains(t, out, "## ✅ Schema Change Applied — Staging")
	assert.NotContains(t, out, "Schema Change Status", "the summary is a verdict, not a status snapshot")
	assert.Contains(t, out, "**Shards**: 2 completed")
	assert.Contains(t, out, "**Duration**: 28m")
	assert.Contains(t, out, "| `-40` | ✅ completed |")
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

	out := formatApplySummaryComment(apply, ops, false, nil, nil, nil, "")

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

	out := formatApplySummaryComment(apply, ops, false, nil, nil, nil, "")

	assert.Contains(t, out, "## ❌ Schema Change Failed — Staging")
	assert.Contains(t, out, "> ⚠️ **Failure:** finalize vschema: apply vschema to keyspace: context deadline exceeded",
		"the apply row's error reaches the callout when no shard carries the failure")
	assert.NotContains(t, out, "First failure:", "no shard failed, so there is no shard failure callout")
}
