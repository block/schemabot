package templates

import (
	"log/slog"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// ProgressData contains data for rendering schema change progress.
type ProgressData struct {
	ApplyID        string
	Database       string
	Environment    string
	Caller         string
	PullRequestURL string
	State          string
	Engine         string
	ErrorMessage   string
	StartedAt      string // RFC3339 format
	CompletedAt    string // RFC3339 format
	Step           int
	StepsTotal     int
	Statement      string
	BuildWork      apitypes.BuildWork
	Operations     []ProgressOperation
	Tables         []TableProgress
	Options        map[string]string // Apply options (defer_cutover, skip_revert, etc.)
	Metadata       map[string]string // Engine metadata (e.g., deploy_request_url, branch_name)
	// Released is true when an operator has released a paused rollout open, so a
	// deployment that failed under on_failure=pause no longer holds later
	// deployments. Apply-level: it applies to every operation of the apply.
	Released bool
	// Sharded is true when the apply is one change fanned out across its
	// keyspaces' shards, its tables already rolled up across them, so it
	// renders as one change, the way its PR comments do, however many shard
	// and finalizer rows it has.
	Sharded bool
	// MaxOperationsAtOnce is how many of the apply's operations the server
	// runs at once, stated beside a running rollout's counts. Zero when the
	// server does not say.
	MaxOperationsAtOnce int
	// StoppedByFreeze names the freeze that stopped the apply, nil when an
	// operator stopped it or it is not stopped.
	StoppedByFreeze *apitypes.FreezeStopRef
}

// RendersOperationSections reports whether progress renders a section per
// operation: an apply with more than one operation does, unless it is a
// sharded apply, which reads as one change.
func RendersOperationSections(ops []ProgressOperation, sharded bool) bool {
	return len(ops) > 1 && !sharded
}

// ProgressOperation represents progress for one deployment operation.
type ProgressOperation struct {
	Deployment          string
	OperationKey        string
	ExternalID          string
	ExternalOperationID string
	OperationKind       string
	Target              string
	State               string
	CutoverPolicy       string
	OnFailure           string
	ErrorMessage        string
	ErrorCode           string
	AlreadyConverged    bool
	// EndedBy names the operation whose end stopped this one at the cutover,
	// when the operation was failed for it rather than on its own.
	EndedBy *presentation.OperationRef
	// RolloutStep is the table step the operation runs when the rollout runs
	// table by table, and 0 otherwise.
	RolloutStep int
	StartedAt   string
	CompletedAt string
}

// TableProgress represents progress for a single table schema change.
type TableProgress struct {
	TableName  string
	Deployment string
	// Target is the address within Deployment this table's copy ran against. One
	// deployment can address several targets, each copying the same table
	// separately, so a section scoped to a deployment alone would merge them.
	Target          string
	Namespace       string // Keyspace (Vitess) or schema name (MySQL)
	Dialect         schema.Dialect
	ChangeType      string // create, alter, drop
	DDL             string
	Status          string
	RowsCopied      int64
	RowsTotal       int64
	PercentComplete int
	ETASeconds      int64
	// BuildWork is the build work the operation that runs this table last
	// reported. While it is a concurrent index build, the build has no rows to
	// count, so a running table draws its bar from PercentComplete, the
	// engine's whole-build percent, and writes the phase's counters where a
	// row copy writes its rows.
	BuildWork apitypes.BuildWork
	// EstimatedBytes is the table's on-disk size when it was planned, shown
	// beside the row counts. Nil when the plan had no estimate.
	EstimatedBytes *int64
	// EstimatedRows is the table's row count when it was planned, shown while
	// the table is queued and before the engine reports RowsTotal. Nil when
	// the plan had no estimate.
	EstimatedRows *int64
	// RowsTotalPlanned marks a RowsTotal that includes the plan's estimate
	// for the targets, or a sharded table's shards, that have not reported
	// their copy yet, so the total renders as approximate.
	RowsTotalPlanned bool
	// PlannedShards is how many shards a sharded table's change spans per its
	// plan, the shards EstimatedBytes covers. Zero when the plan recorded none.
	PlannedShards int
	// Checksum phase progress: rows verified so far and total to verify.
	// Non-zero only while the table is checksumming (verifying copied data).
	ChecksumRowsChecked int64
	ChecksumRowsTotal   int64
	// The engine's throttler is pausing this table's active phase (row copy
	// or checksum verification). ThrottleReason names the signal for display and is
	// empty when Throttled is false.
	Throttled      bool
	ThrottleReason string
	IsInstant      bool
	// Shards is the table's per-part progress: one entry per shard, or one per
	// target when AcrossTargets is set.
	Shards []ShardProgress
	// AcrossTargets marks a table that stands for one change across a
	// rollout's targets, rolled up the way a sharded table rolls up its shards.
	AcrossTargets bool
	// OnTargets names the targets a rolled-up table's DDL runs on when they
	// are only some of the deployment's, shown above the DDL. Empty when the
	// DDL runs on every target.
	OnTargets string
	// RolloutSettled marks a rolled-up table whose rollout has settled with no
	// target left to run, so a target still pending on it never starts it and
	// reads as not started rather than queued.
	RolloutSettled bool
	// ApplyFailed marks a table of an apply that ended failed. A table such an
	// apply recorded cancelled before it started never ran because an earlier
	// one failed, not because anyone cancelled it, so it reads as not run.
	ApplyFailed bool
	// Started reports that the engine began the table's change, which a step
	// with no row progress, such as a validation, shows no other way.
	Started bool
	// Resuming marks a table of a resuming apply. While no target has moved
	// it on from where the stop left it (presentation.AwaitingResume), its
	// stopped progress is from before the stop and whether the copy continues
	// from its checkpoint is not known yet, so it reads as resuming without a
	// percent.
	Resuming bool
}

// ShardProgress contains per-shard progress for template rendering.
type ShardProgress struct {
	Shard           string
	Status          string
	RowsCopied      int64
	RowsTotal       int64
	ETASeconds      int64
	PercentComplete int
	CutoverAttempts int
	// ChecksumRowsChecked and ChecksumRowsTotal are how far a checksumming
	// target's verify has got, once its engine reports a total.
	ChecksumRowsChecked int64
	ChecksumRowsTotal   int64
	// PlannedRows is the plan's row estimate for a target in a table rolled up
	// across targets whose copy is still ahead and has reported no row total,
	// standing in for its rows in the rolled-up total
	// (TableProgress.RowsTotalPlanned). Nil otherwise.
	PlannedRows *int64
	// CopiesNoRows is true when every change the shard runs on the table
	// copies no rows, so a running shard reads as applying.
	CopiesNoRows bool
}

// Display-only task states. These are not persisted apply states (see pkg/applystate)
// but are used for per-table rendering in sequential mode.
const (
	TaskCancelled = "cancelled" // Table was never executed due to earlier failure
)

// ParseProgressResponse converts a typed ProgressResponse to ProgressData for
// rendering. Failure and throttle text is relayed from the engine and the
// database, so control runes are removed before it reaches the terminal.
func ParseProgressResponse(result *apitypes.ProgressResponse) ProgressData {
	data := ProgressData{
		ApplyID:        result.ApplyID,
		Database:       result.Database,
		Environment:    result.Environment,
		Caller:         result.Caller,
		PullRequestURL: result.PullRequest,
		State:          state.NormalizeState(result.State),
		Engine:         result.Engine,
		ErrorMessage:   ui.TerminalSafe(result.ErrorMessage),
		StartedAt:      result.StartedAt,
		CompletedAt:    result.CompletedAt,
		Options:        result.Options,
		Metadata:       result.Metadata,
		Released:       result.Released,
		Sharded:        result.Sharded,

		MaxOperationsAtOnce: result.MaxOperationsAtOnce,

		StoppedByFreeze: result.StoppedByFreeze,
	}
	if step, err := apitypes.ParseProgressStep(result.Metadata); err != nil {
		slog.Warn("progress output omits the statement position because the progress metadata is malformed", "apply_id", result.ApplyID, "error", err)
	} else {
		data.Step, data.StepsTotal, data.Statement = step.Step, step.StepsTotal, step.Statement
	}
	work, err := apitypes.ParseBuildWork(result.Metadata)
	if err != nil {
		slog.Warn("progress output omits part of the build work because the progress metadata is malformed", "apply_id", result.ApplyID, "error", err)
	}
	data.BuildWork = work
	dialect := schema.DialectForDatabaseType(result.DatabaseType)

	workByOperation := make(map[presentation.OperationRef]apitypes.BuildWork, len(result.Operations))
	for _, op := range result.Operations {
		work, err := apitypes.ParseBuildWork(op.Metadata)
		if err != nil {
			slog.Warn("progress output omits part of an operation's build work because its progress metadata is malformed",
				"apply_id", result.ApplyID, "deployment", op.Deployment, "operation_key", op.OperationKey, "error", err)
		}
		workByOperation[presentation.OperationRef{Deployment: op.Deployment, OperationKey: op.OperationKey}] = work
		data.Operations = append(data.Operations, ProgressOperation{
			Deployment:          op.Deployment,
			OperationKey:        op.OperationKey,
			ExternalID:          op.ExternalID,
			ExternalOperationID: op.ExternalOperationID,
			OperationKind:       op.OperationKind,
			Target:              op.Target,
			State:               state.NormalizeState(op.State),
			CutoverPolicy:       op.CutoverPolicy,
			OnFailure:           op.OnFailure,
			ErrorMessage:        ui.TerminalSafe(op.ErrorMessage),
			ErrorCode:           op.ErrorCode,
			AlreadyConverged:    op.AlreadyConverged,
			EndedBy:             progressOperationRef(op.EndedBy),
			RolloutStep:         op.RolloutStep,
			StartedAt:           op.StartedAt,
			CompletedAt:         op.CompletedAt,
		})
	}

	if data.Sharded && data.ErrorMessage == "" {
		// A sharded apply shows no operation sections, so a failure only one
		// of its operations recorded (a failed shard or finalize) is the
		// apply's.
		data.ErrorMessage = firstOperationError(data.Operations)
	}

	for _, tbl := range result.Tables {
		tp := TableProgress{
			TableName:           tbl.TableName,
			Deployment:          tbl.Deployment,
			Target:              tbl.Target,
			Namespace:           tbl.Keyspace,
			Dialect:             dialect,
			ChangeType:          tbl.ChangeType,
			DDL:                 tbl.DDL,
			Status:              state.NormalizeTaskStatus(tbl.Status),
			RowsCopied:          tbl.RowsCopied,
			RowsTotal:           tbl.RowsTotal,
			RowsTotalPlanned:    tbl.RowsTotalPlanned,
			EstimatedBytes:      tbl.EstimatedBytes,
			EstimatedRows:       tbl.EstimatedRows,
			PlannedShards:       int(tbl.PlannedShards),
			PercentComplete:     int(tbl.PercentComplete),
			ETASeconds:          tbl.ETASeconds,
			ChecksumRowsChecked: tbl.ChecksumRowsChecked,
			ChecksumRowsTotal:   tbl.ChecksumRowsTotal,
			Throttled:           tbl.Throttled,
			ThrottleReason:      ui.TerminalSafe(tbl.ThrottleReason),
			IsInstant:           tbl.IsInstant,
			ApplyFailed:         state.IsState(data.State, state.Apply.Failed),
			Started:             tbl.StartedAt != "",
		}
		copies := make([]presentation.ShardCopy, 0, len(tbl.Shards))
		for _, sh := range tbl.Shards {
			pct := int(sh.PercentComplete)
			if pct == 0 && sh.RowsTotal > 0 {
				pct = int(sh.RowsCopied * 100 / sh.RowsTotal)
			}
			// Row totals are estimates, so a nearly finished copy can exceed
			// them whether the percent arrived from the server or was derived
			// above; clamp so the rendered percent stays honest.
			pct = ui.ClampPercent(pct)
			tp.Shards = append(tp.Shards, ShardProgress{
				Shard:           sh.Shard,
				Status:          state.NormalizeShardStatus(sh.Status),
				RowsCopied:      sh.RowsCopied,
				RowsTotal:       sh.RowsTotal,
				ETASeconds:      sh.ETASeconds,
				PercentComplete: pct,
				CutoverAttempts: int(sh.CutoverAttempts),

				ChecksumRowsChecked: sh.ChecksumRowsChecked,
				ChecksumRowsTotal:   sh.ChecksumRowsTotal,
				CopiesNoRows:        sh.CopiesNoRows,
			})
			copies = append(copies, presentation.ShardCopy{RowsCopied: sh.RowsCopied, RowsTotal: sh.RowsTotal, ETASeconds: sh.ETASeconds})
		}
		if tp.ETASeconds == 0 {
			// The server reports a sharded table's ETA when it has one, which
			// the PR comment shows too. Without it, the table's ETA is the
			// slowest shard's, as the server reads it. The response carries
			// no per-shard throttle, but the server reports an ETA whenever a
			// shard does, so this reading never has a throttled shard to
			// leave out.
			tp.ETASeconds = presentation.ShardedTableETA(copies)
		}
		tp.BuildWork = tableBuildWork(data.BuildWork, workByOperation, len(result.Operations), tbl)
		data.Tables = append(data.Tables, tp)
	}

	return data
}

// tableBuildWork is the work in flight on the operation that runs tbl. The
// top-level metadata speaks for one operation, so it is the table's only when
// the apply has one operation; with several, the table reads its own
// operation's, and a table no listed operation claims has none.
func tableBuildWork(applyWork apitypes.BuildWork, byOperation map[presentation.OperationRef]apitypes.BuildWork, operations int, tbl *apitypes.TableProgressResponse) apitypes.BuildWork {
	if operations <= 1 {
		return applyWork
	}
	return byOperation[presentation.OperationRef{Deployment: tbl.Deployment, OperationKey: tbl.OperationKey}]
}

// firstOperationError names the first failure among a sharded apply's
// operations, or returns "" when none failed: a failed shard first, then a
// failed keyspace finalize, then any other failed operation's error. It names
// the shard or keyspace, as the PR comment's failure callout does, since the
// apply shows no section per operation to say where it failed.
func firstOperationError(ops []ProgressOperation) string {
	// Shard failures come first and a finalizer's after them, the order the
	// PR comment's failure callout names them in.
	for _, op := range ops {
		if !isOperationFailure(op) {
			continue
		}
		if _, shard, _, ok := state.ShardWorkKey(op.OperationKey); ok {
			if op.ErrorMessage == "" {
				return "shard " + shard + " failed"
			}
			return "shard " + shard + ": " + op.ErrorMessage
		}
	}
	for _, op := range ops {
		if !isOperationFailure(op) {
			continue
		}
		if keyspace, ok := state.NamespaceFinalizerKey(op.OperationKey); ok {
			// The finalize error need not name its keyspace, so the line does.
			if op.ErrorMessage == "" {
				return "finalizing keyspace " + keyspace + " failed"
			}
			return "finalizing keyspace " + keyspace + ": " + op.ErrorMessage
		}
	}
	for _, op := range ops {
		if isOperationFailure(op) && op.ErrorMessage != "" {
			return op.ErrorMessage
		}
	}
	return ""
}

// isOperationFailure reports whether an operation failed or is retrying a
// failure.
func isOperationFailure(op ProgressOperation) bool {
	return state.IsState(op.State, state.ApplyOperation.Failed, state.ApplyOperation.FailedRetryable)
}

// progressOperationRef maps the API's reference to an operation of the apply
// to the presentation's, keeping an absent reference absent.
func progressOperationRef(ref *apitypes.ProgressOperationRef) *presentation.OperationRef {
	if ref == nil {
		return nil
	}
	return &presentation.OperationRef{Deployment: ref.Deployment, OperationKey: ref.OperationKey}
}
