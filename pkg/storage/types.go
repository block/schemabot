package storage

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/schema"
)

// Cutover policies control how a multi-deployment rollout sequences the copy
// and cutover phases of its deployments. The value is resolved from the
// environment config at apply-create time and persisted on each apply_operations
// row so the policy in force when the apply was created travels with it.
const (
	// CutoverPolicyRolling is the default: a later deployment does not start
	// until every earlier sibling in deployment_order has completed, keeping the
	// rollout fully serial.
	CutoverPolicyRolling = "rolling"

	// CutoverPolicyBarrier lets later deployments run their copy phase once
	// earlier siblings reach the cutover barrier, while cutover stays ordered.
	CutoverPolicyBarrier = "barrier"

	// CutoverPolicyParallel drops copy-phase ordering entirely: every deployment
	// copies concurrently from the start, with no earlier-sibling gate on copy
	// start. Only the cutover phase stays deployment-ordered, exactly like
	// barrier. This collapses copy wall-clock toward "longest copy" for rollouts
	// whose hours-long copy dominates, while preserving the ordered, one-at-a-time
	// cutover swaps.
	CutoverPolicyParallel = "parallel"
)

// IsOrderedCutoverPolicy reports whether a cutover policy parks each
// multi-deployment operation at the cutover barrier and drives the high-risk
// swaps in deployment order via the cutover-claim path. Both barrier and
// parallel share this ordered-cutover behaviour; they differ only in the
// copy-start gate (barrier staggers copies, parallel runs them concurrently).
// rolling and any unrecognized value are not ordered-cutover policies.
func IsOrderedCutoverPolicy(policy string) bool {
	return policy == CutoverPolicyBarrier || policy == CutoverPolicyParallel
}

// On-failure policies control multi-deployment rollout continuation when an
// earlier deployment terminally fails. Like the cutover policy, the value is
// resolved from the environment config at apply-create time and persisted on
// each apply_operations row so the policy in force when the apply was created
// travels with it.
const (
	// OnFailureHalt is the default: a terminal-failed earlier deployment blocks
	// every later deployment of the same apply, so the rollout stops at the
	// first failure.
	OnFailureHalt = "halt"

	// OnFailureContinue treats a terminal-failed earlier deployment as settled
	// so it no longer blocks later deployments; the rollout attempts every
	// deployment instead of stopping at the first failure.
	OnFailureContinue = "continue"

	// OnFailurePause holds the rollout after a failure until a human releases it
	// (via the release control op) so the remaining deployments proceed; to
	// abort instead, use the separate stop/cancel control op. Until released,
	// later deployments do not start and the apply reports the non-terminal
	// paused state; the merge gate stays fail-closed on the failed deployment.
	OnFailurePause = "pause"
)

// Lock represents a database-level deployment lock.
// Locks prevent concurrent schema changes to the same database across
// all environments and PRs. Lock key is database:type (no environment).
type Lock struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// DatabaseName is the name of the database being locked.
	DatabaseName string

	// DatabaseType is the type of database: "vitess" or "mysql".
	DatabaseType string

	// Repository is the GitHub repository (owner/repo format).
	Repository string

	// PullRequest is the PR number that holds the lock (0 for CLI).
	PullRequest int

	// Owner identifies who holds the lock.
	// For PR-based: "repo#pr" (e.g., "block/myapp#123")
	// For CLI: "cli:user@hostname" or similar
	Owner string

	// PendingPlanID is the plan_identifier of the apply-confirmation plan that
	// posted this lock. apply-confirm loads this exact plan to evaluate the
	// cross-delivery freshness invariant, instead of guessing from "newest plan
	// for repo+pr+env+database" (which can pick up plain `schemabot plan`
	// results posted after the confirmation plan).
	//
	// Empty when the lock was acquired outside the apply path (rollback, CLI
	// unlock/lock, or a row written before this column existed). The confirm
	// path treats empty as "skip the freshness check" rather than fail closed.
	PendingPlanID string

	// CreatedAt is when the lock was acquired.
	CreatedAt time.Time

	// UpdatedAt is when the lock was last updated.
	UpdatedAt time.Time
}

// Check terminology:
//   - GitHub Check Run: the external GitHub Checks API object visible on a PR.
//   - Stored check state: a row in SchemaBot's checks table.
//
// Check represents stored check state. SchemaBot writes per-database stored
// state when planning, starting an apply, completing an apply, reconciling stale
// applies, or processing rollback completion. Those per-database rows do not
// create per-database GitHub Check Runs; they are internal inputs to aggregate
// calculation.
//
// SchemaBot stores this state because GitHub only owns the visible merge-gate
// object, not SchemaBot's per-database workflow state. Durable stored state lets
// SchemaBot recompute aggregates, survive restarts, reconcile stale applies,
// enforce ordering rules, clean up on PR close, and answer internal safety
// checks without treating the GitHub API as the source of truth.
//
// The aggregate check path reads the per-database stored state, creates or
// updates the visible GitHub Check Run, then writes an aggregate stored state
// row whose CheckRunID points at that GitHub object. Later GitHub check_run
// webhooks use CheckRunID to find the matching stored state.

// PlanDriftState declares how a plan-result write treats a stored review-time
// deployment drift block. It lets UpsertPlanResult tell "the drift rollup ran
// and this deployment set is clean" apart from "drift was not evaluated on this
// write" — two cases that both carry an empty BlockingReason but must clear vs
// preserve an existing drift block respectively.
type PlanDriftState int

const (
	// PlanDriftNotEvaluated means the write did not run the drift rollup (e.g. an
	// apply-time plan). It is the zero value so an unset drift argument fails
	// safe: an existing drift block is preserved, never silently cleared.
	PlanDriftNotEvaluated PlanDriftState = iota
	// PlanDriftClean means the rollup ran and every deployment matched the
	// reviewed plan, so a stale drift block may be cleared.
	PlanDriftClean
	// PlanDriftBlocked means the rollup ran and a deployment diverged or could
	// not be confirmed, so the write records the drift block.
	PlanDriftBlocked
)

// ReviewTimeDeploymentDriftBlockingReason is the stable Check.BlockingReason
// value for a review-time deployment drift block. It is defined here so the
// plan-result write guard and the webhook block reason share one source of
// truth: UpsertPlanResult preserves a row carrying this reason on a
// not-evaluated write instead of clearing it.
const ReviewTimeDeploymentDriftBlockingReason = "review_time_deployment_drift"

type Check struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// Repository is the GitHub repository (owner/repo format).
	Repository string

	// PullRequest is the PR number.
	PullRequest int

	// HeadSHA is the commit SHA the check is associated with.
	HeadSHA string

	// Environment is the target environment: "staging" or "production".
	Environment string

	// DatabaseType is the database type: "vitess" or "mysql".
	DatabaseType string

	// DatabaseName is the name of the database.
	DatabaseName string

	// CheckRunID is the GitHub Check Run ID used to update GitHub via the Checks API.
	CheckRunID int64

	// ApplyID is the storage apply ID this stored state currently represents.
	// It is set while apply state is in_progress so terminal updates cannot
	// overwrite a newer apply's stored check state.
	ApplyID int64

	// HasChanges indicates whether schema changes were detected.
	HasChanges bool

	// Status is SchemaBot's stored status: "pending_apply", "applying", "completed", etc.
	Status string

	// Conclusion is the GitHub Check Run conclusion set when the check completes.
	// Values: "success", "failure", "action_required", "neutral", "cancelled", "skipped".
	Conclusion string

	// BlockingReason is a machine-readable reason code explaining why stored
	// check state must keep the aggregate check from passing.
	// ErrorMessage remains human-readable display text.
	BlockingReason string

	// ErrorMessage contains a human-readable explanation when the check state fails.
	// Displayed in the GitHub Check Run details and PR comment.
	ErrorMessage string

	// ChangeSummary is a human-readable per-database summary of the planned
	// schema change (e.g. "5 created, 3 altered · 2 vschema updates"). It is set
	// at plan time and preserved across apply-state transitions. Empty for
	// aggregate rows and for rows recorded before this field existed.
	ChangeSummary string

	// CreatedAt is when the check was created.
	CreatedAt time.Time

	// UpdatedAt is when the check was last updated.
	UpdatedAt time.Time
}

// CheckStatus constants.
const (
	CheckStatusPending = "pending"
	CheckStatusSuccess = "success"
	CheckStatusFailure = "failure"
)

// Setting represents an admin-level SchemaBot setting.
// Examples: feature flags, default options, maintenance mode.
type Setting struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// Key is the unique setting name.
	Key string

	// Value is the setting value. Use JSON for complex values.
	Value string

	// CreatedAt is when the setting was created.
	CreatedAt time.Time

	// UpdatedAt is when the setting was last updated.
	UpdatedAt time.Time
}

// DatabaseType constants.
const (
	DatabaseTypeVitess = "vitess"
	DatabaseTypeMySQL  = "mysql"
	DatabaseTypeStrata = "strata"
)

// Engine constants.
const (
	EngineSpirit      = "spirit"
	EnginePlanetScale = "planetscale"
	EngineStrata      = "strata"
)

// ApplyOperationKind constants classify operation rows by scheduling role.
const (
	ApplyOperationKindWork           = "work"
	ApplyOperationKindGroupFinalizer = "group_finalizer"
)

// EngineForType returns the engine name for a database type.
func EngineForType(dbType string) string {
	switch dbType {
	case DatabaseTypeVitess:
		return EnginePlanetScale
	case DatabaseTypeStrata:
		return EngineStrata
	default:
		return EngineSpirit
	}
}

// TableChange represents a DDL change to a single table.
type TableChange struct {
	// Namespace is the schema name (MySQL) or keyspace (Vitess).
	Namespace string `json:"namespace,omitempty"`

	// Table is the table name.
	Table string `json:"table"`

	// DDL is the DDL statement to execute.
	DDL string `json:"ddl"`

	// Operation is "create", "alter", or "drop".
	Operation string `json:"operation"`

	// IsUnsafe records whether the planner marked this change unsafe.
	IsUnsafe bool `json:"is_unsafe,omitempty"`

	// UnsafeReason records the planner's reason for unsafe changes.
	UnsafeReason string `json:"unsafe_reason,omitempty"`
}

// RequiresUnsafeOptIn reports whether applying this change requires explicit
// unsafe opt-in. Stored plans keep the planner's unsafe metadata; drop remains
// fail-closed so older plans without the metadata cannot queue table deletion as
// safe.
func (tc TableChange) RequiresUnsafeOptIn() bool {
	return tc.IsUnsafe || strings.EqualFold(tc.Operation, "drop")
}

// UnsafeOptInReason returns the planner-provided unsafe reason, or a generic
// table-drop reason for older/malformed plans that only persisted the operation.
func (tc TableChange) UnsafeOptInReason() string {
	if tc.UnsafeReason != "" {
		return tc.UnsafeReason
	}
	if strings.EqualFold(tc.Operation, "drop") {
		return "DROP TABLE removes all data"
	}
	return "unsafe schema change requires explicit opt-in"
}

// NamespacePlanData contains plan data for a single namespace. OriginalFiles is
// captured once for the namespace and applies to every table/artifact change in
// Tables and Artifacts.
type NamespacePlanData struct {
	Tables                []TableChange     `json:"tables,omitempty"`
	Shards                []ShardPlan       `json:"shards,omitempty"`
	OriginalFiles         map[string]string `json:"original_files,omitempty"`
	OriginalFilesCaptured bool              `json:"original_files_captured,omitempty"`
	Artifacts             map[string]string `json:"artifacts,omitempty"`
}

// ShardPlan records per-shard membership and drift captured at plan time for a
// sharded namespace. It is generic storage metadata used by apply-create to
// reconstruct operation groups after the original plan request has returned.
type ShardPlan struct {
	Shard     string `json:"shard"`
	Namespace string `json:"namespace,omitempty"`
	// Changes are this shard's own table changes; a shard is changing when this
	// is non-empty. Persisting the changes (rather than a separate membership
	// flag) lets a keyspace whose shards diverge — drift, or a partially-applied
	// canary rollout — be represented per shard, and makes the reviewed DDL the
	// exact DDL that gets applied.
	Changes []TableChange `json:"changes,omitempty"`
}

// Plan represents a schema change plan generated by tern.Client.Plan().
// Plans are immutable after creation - they capture the schema state at plan time.
// SchemaBot stores plans so both GRPCClient and LocalClient can be stateless.
type Plan struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// PlanIdentifier is the external identifier for API (e.g., "plan_abc123").
	PlanIdentifier string

	// Database is the target database name.
	Database string

	// DatabaseType is "vitess" or "mysql".
	DatabaseType string

	// Deployment is the Tern deployment selected by server config at plan time.
	Deployment string

	// Target is the Tern-facing target selected by server config at plan time.
	Target string

	// Repository is the GitHub repository (owner/repo format).
	Repository string

	// PullRequest is the PR number that generated this plan.
	PullRequest int

	// SchemaPath is the repo-relative directory that generated this plan.
	SchemaPath string

	// Environment is "staging" or "production".
	Environment string

	// SchemaFiles contains the input schema files organized by namespace.
	// Stored as JSON for audit trail and DDL re-generation if needed.
	SchemaFiles schema.SchemaFiles

	// Namespaces contains per-namespace plan data: table changes, original files,
	// shard metadata, and engine-specific desired artifacts keyed by filename.
	// The key is the database/schema name for MySQL, or keyspace name for Vitess.
	Namespaces map[string]*NamespacePlanData

	// Shards is the flattened per-shard membership and drift captured at plan
	// time. PlanStore stores this under Namespaces so plan_data remains
	// namespace-keyed; the flattened field is the convenient read/write API.
	// Because shard metadata is optional, old plan_data rows without shards still
	// decode cleanly.
	Shards []ShardPlan

	// HeadSHA is the PR HEAD SHA at the time the plan was rendered. It is the
	// durable record of "which commit did the user actually review". apply-confirm
	// compares this against the current PR HEAD (via FetchPullRequestNoCache) to
	// catch the cross-delivery race where HEAD advances between the plan being
	// posted and the user clicking apply-confirm.
	//
	// Empty for plans created before this column existed. Callers that enforce the
	// cross-delivery freshness invariant must treat an empty value as "skip" (the
	// invariant cannot be evaluated) rather than fail closed.
	HeadSHA string

	// CreatedAt is when the plan was generated.
	CreatedAt time.Time
}

// FlatDDLChanges returns all DDL changes across namespaces, sorted by namespace key.
func (p *Plan) FlatDDLChanges() []TableChange {
	if len(p.Namespaces) == 0 {
		return nil
	}
	keys := make([]string, 0, len(p.Namespaces))
	for k := range p.Namespaces {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var result []TableChange
	for _, k := range keys {
		for _, tc := range p.Namespaces[k].Tables {
			if tc.Namespace == "" {
				tc.Namespace = k
			}
			result = append(result, tc)
		}
	}
	return result
}

// UnsafeDDLChanges returns stored DDL changes that require explicit unsafe
// opt-in before queueing operator work.
func (p *Plan) UnsafeDDLChanges() []TableChange {
	var result []TableChange
	appendUnsafe := func(tc TableChange) {
		if tc.RequiresUnsafeOptIn() {
			result = append(result, tc)
		}
	}
	for _, tc := range p.FlatDDLChanges() {
		appendUnsafe(tc)
	}
	for _, shard := range p.Shards {
		for _, tc := range shard.Changes {
			if tc.Namespace == "" {
				tc.Namespace = shard.Namespace
			}
			appendUnsafe(tc)
		}
	}
	return result
}

// HasOriginalFilesCapture reports whether every stored namespace has an
// explicit original-files capture, including intentionally empty captures.
func (p *Plan) HasOriginalFilesCapture() bool {
	if p == nil || len(p.Namespaces) == 0 {
		return false
	}
	for _, ns := range p.Namespaces {
		if ns == nil || !ns.OriginalFilesCaptured {
			return false
		}
	}
	return true
}

// Apply represents a schema change execution from SchemaBot's perspective.
// Created when Apply() is called, updated during execution.
// Engine-specific state is stored in Tern (either via gRPC or LocalClient storage).
//
// Naming: "Apply" matches the CLI command (`schemabot apply`) and natural speech
// ("the apply is running"). Each Apply contains one or more Tasks (individual DDLs)
// in SchemaBot's storage.
type Apply struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// ApplyIdentifier is the external identifier for API (e.g., "apply_abc123").
	// Used in API responses and CLI commands.
	ApplyIdentifier string

	// LockID points to locks.id.
	LockID int64

	// PlanID points to plans.id.
	PlanID int64

	// Database is the target database name (denormalized from lock for queries).
	Database string

	// DatabaseType is "vitess" or "mysql" (denormalized from lock for queries).
	DatabaseType string

	// Repository is the GitHub repository (denormalized from lock for GetByPR).
	Repository string

	// PullRequest is the PR number (denormalized from lock for GetByPR).
	PullRequest int

	// Environment is "staging" or "production".
	Environment string

	// Deployment is the Tern deployment name used for this apply.
	// Local mode stores the database name so recovery and controls can route
	// using the same plan-time decision as gRPC mode.
	Deployment string

	// Caller identifies who initiated this apply.
	// For CLI: "cli:user@hostname", for PR: "repo#pr"
	Caller string

	// InstallationID is the GitHub App installation ID (for webhook-triggered applies).
	InstallationID int64

	// ExternalID is the remote engine's identifier for this apply.
	// For gRPC mode: the remote Tern's apply_id (the remote engine's apply identifier).
	// Empty for local mode (SchemaBot IS the engine).
	ExternalID string

	// IdempotencyKey deduplicates remote apply dispatch. The control plane stamps
	// a deterministic key per dispatch generation onto the data-plane apply it
	// creates, so a re-dispatch after a lost response returns the same apply
	// instead of starting a duplicate. Empty (stored as NULL) for applies that
	// are not created through an idempotent remote dispatch (CLI, local mode).
	IdempotencyKey string

	// Engine is the schema change engine: "spirit", "planetscale", etc.
	Engine string

	// State is the current execution state.
	State string

	// ErrorMessage contains error details if state is failed.
	ErrorMessage string

	// Options contains durable apply options and operator metadata as JSON.
	// Use ParseApplyOptions() to get typed access.
	Options []byte

	// Attempt tracks operator retry attempts for failed_retryable applies.
	// Once the retry budget is exhausted, the apply becomes failed.
	Attempt int

	// LeaseOwner identifies the driver that last claimed this apply. It is
	// operator-facing context; LeaseToken is the ownership capability used for
	// correctness.
	LeaseOwner string

	// LeaseToken is rotated on each claim. Owned writes must include the current
	// token to avoid stale drivers overwriting a newer owner.
	LeaseToken string

	// LeaseAcquiredAt records when the current lease was acquired.
	LeaseAcquiredAt *time.Time

	// CreatedAt is when the apply was created.
	CreatedAt time.Time

	// StartedAt is when the apply started running.
	StartedAt *time.Time

	// CompletedAt is when the apply reached a terminal state.
	CompletedAt *time.Time

	// RevertSkippedAt records when skip-revert was dispatched for this apply.
	// Non-nil means revert was skipped and finalization is in progress; it is
	// core control state surfaced to progress consumers, set by both the
	// control-plane skip-revert handler and the data-plane finalizer.
	RevertSkippedAt *time.Time

	// UpdatedAt is when the apply was last updated.
	UpdatedAt time.Time
}

// Lease returns the ownership token for this apply.
func (a *Apply) Lease() ApplyLease {
	if a == nil {
		return ApplyLease{}
	}
	return ApplyLease{
		ApplyID: a.ID,
		Owner:   a.LeaseOwner,
		Token:   a.LeaseToken,
	}
}

// IsRollback reports whether this apply reverts a previously applied schema
// change. It reads the durable rollback option so any terminal path can tell a
// rollback from an ordinary apply without the rollback command's in-memory
// context.
func (a *Apply) IsRollback() bool {
	if a == nil {
		return false
	}
	return ParseApplyOptions(a.Options).Rollback
}

// ApplyOperation represents one child row in the apply_operations table:
// the per-(deployment, operation_key) slice of a multi-operation apply, and the
// unit of work the driver claim loop reconciles.
//
// In the multi-operation data model, one applies row owns 1..N
// apply_operations rows. Each row carries its
// own state machine (mirroring state.Apply, so cutover/stop/revert/triage
// work per-row), its own lock identity, and its own progress; the driver
// can act on each operation independently while keeping `apply_id` as the
// user-facing handle for the whole rollout. applies.state is derived from
// the child rows' states via the existing state.DeriveApplyState().
type ApplyOperation struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// ApplyID points to applies.id. Unique together with Deployment and
	// OperationKey.
	ApplyID int64

	// Deployment is the Tern deployment name this child row targets
	// (e.g. "region-a", "payments-eu"). Drawn from the resolved
	// environment-level deployments map in server config.
	Deployment string

	// OperationKey disambiguates multiple execution operations in the same apply
	// and deployment. Empty is the legacy single-operation key.
	OperationKey string

	// OperationKind classifies the operation's scheduling role. The default
	// "work" kind is ordinary engine work; "group_finalizer" runs after its
	// grouped work siblings succeed.
	OperationKind string

	// Target is the Tern-facing address resolved for this deployment at apply
	// time. Mirrors plans.target / applies.target
	// semantics. Empty for legacy / single-deployment shapes.
	Target string

	// ExternalID is the remote data plane's stable apply identifier for this
	// operation. It is scoped to the operation because a multi-operation parent
	// apply has no single authoritative remote apply identifier.
	ExternalID string

	// ExternalOperationID is the remote data plane's numeric operation row ID for
	// this operation. Empty for local applies and for remote data planes that do
	// not return operation IDs.
	ExternalOperationID string

	// State is the per-operation state machine value. See state.ApplyOperation.
	State string

	// ErrorMessage contains error details if state is failed.
	ErrorMessage string

	// CutoverPolicy is the rollout sequencing policy captured for this
	// operation's parent apply at apply-create time, drawn from the resolved
	// environment config. "rolling" (the default) keeps the fully serial
	// rollout — a later deployment waits for every earlier sibling to complete.
	// "barrier" allows later deployments to run their copy phase once earlier
	// siblings reach the cutover barrier. Persisted on each row so the policy in
	// force when the apply was created travels with the operation. Insert treats
	// an empty value as "rolling", matching the column's NOT NULL DEFAULT.
	CutoverPolicy string

	// OnFailure is the rollout-continuation policy captured for this operation's
	// parent apply at apply-create time, drawn from the resolved environment
	// config. "halt" (the default) blocks every later deployment of the same
	// apply once an earlier sibling terminally fails — the rollout stops at the
	// first failure. "continue" treats a terminal-failed earlier sibling as
	// settled so it no longer blocks later siblings, and the rollout attempts
	// every deployment instead of halting. It governs only rollout
	// continuation, never the apply's pass/fail verdict or the merge gate,
	// which stay fail-closed on any failed deployment. Insert treats an empty
	// value as "halt", matching the column's NOT NULL DEFAULT, so a caller that
	// omits the policy never silently degrades to the less-safe behaviour.
	OnFailure string

	// Attempt counts deliberate redispatches of this operation. It starts at 0
	// and advances only when a driver re-claims the row as a deliberate retry —
	// the operation is failed_retryable and so is its parent apply. It does not
	// advance on a crash-recovery re-lease (a still-active parent with a stale
	// heartbeat, or an in-flight drive whose driver died), so an orphaned
	// dispatch's idempotency key stays stable and is reused rather than
	// duplicated. It is the operation-local counterpart to applies.attempt: a
	// sibling operation's retry advances the shared parent attempt but never this
	// row's, so an operation-scoped dispatch generation rotates only on its own
	// retry.
	Attempt int

	// StartedAt is when the operator claimed this child row and execution began.
	StartedAt *time.Time

	// CompletedAt is when this child row reached a non-resumable terminal state
	// (completed, failed, cancelled, reverted). It stays nil for the resumable
	// stopped state, matching the apply-level convention, since stopped work may
	// still resume.
	CompletedAt *time.Time

	// LeaseOwner, LeaseToken, and LeaseAcquiredAt are the operation's own claim
	// lease, rotated when a driver claims the row via FindNextApplyOperation and
	// refreshed by its heartbeat. They mirror the apply-level lease columns so a
	// driver can guard operation-scoped writes on this row's token instead of the
	// parent apply's, letting sibling deployments run independently.
	LeaseOwner      string
	LeaseToken      string
	LeaseAcquiredAt *time.Time

	// EngineResumeContext and EngineResumeMetadata are opaque state owned by the
	// engine package and scoped to this execution operation. SchemaBot stores and
	// replays them but does not interpret them for control/progress calls.
	EngineResumeContext  string
	EngineResumeMetadata string

	// CreatedAt is when the child row was inserted (typically at apply create).
	CreatedAt time.Time

	// UpdatedAt is when the child row was last updated.
	UpdatedAt time.Time
}

// Lease returns the ownership token for this apply_operation.
func (op *ApplyOperation) Lease() OperationLease {
	if op == nil {
		return OperationLease{}
	}
	return OperationLease{
		ApplyID:     op.ApplyID,
		OperationID: op.ID,
		Owner:       op.LeaseOwner,
		Token:       op.LeaseToken,
	}
}

// ApplyOptions contains durable user and engine options for an apply.
// Stored as JSON in the database for flexibility across engine types.
type ApplyOptions struct {
	// AllowUnsafe permits destructive changes (DROP TABLE, DROP COLUMN).
	AllowUnsafe bool `json:"allow_unsafe,omitempty"`

	// Branch is the name of an existing PlanetScale branch to reuse.
	// When set, the engine refreshes the branch schema from main instead
	// of creating a new branch.
	Branch string `json:"branch,omitempty"`

	// DeferCutover pauses at cutover and waits for explicit trigger.
	DeferCutover bool `json:"defer_cutover,omitempty"`

	// DeferDeploy pauses before deploying the deploy request and waits for
	// explicit trigger (PlanetScale only). The user can review the deploy
	// request diff on PlanetScale before proceeding.
	DeferDeploy bool `json:"defer_deploy,omitempty"`

	// SkipRevert skips the revert window after completion (Vitess only).
	SkipRevert bool `json:"skip_revert,omitempty"`

	// Volume controls schema change aggressiveness (1-11).
	Volume int `json:"volume,omitempty"`

	// Target is the opaque endpoint-discovery target forwarded to Tern.
	// Defaults to the apply database when empty.
	Target string `json:"target,omitempty"`

	// Rollback marks an apply that reverts a previously applied schema change
	// (executed from a rollback plan). It is durable so any terminal path can
	// distinguish a rollback from an ordinary apply: a completed rollback must
	// leave the required check action_required (the PR's change has been reverted
	// and must not merge as-is), not success.
	Rollback bool `json:"rollback,omitempty"`
}

// ControlOperation identifies a user-requested control operation.
type ControlOperation string

const (
	// ControlOperationStart resumes a stopped apply.
	ControlOperationStart ControlOperation = "start"
	// ControlOperationStop stops an active apply.
	ControlOperationStop ControlOperation = "stop"
	// ControlOperationCancel terminates an active apply.
	ControlOperationCancel ControlOperation = "cancel"
	// ControlOperationCutover triggers deferred cutover.
	ControlOperationCutover ControlOperation = "cutover"
	// ControlOperationRevert undoes a completed PlanetScale schema change during
	// its revert window. Durable so a comment-driven revert survives the API
	// process dying before the engine call lands, and so the apply owner (which
	// for a remote apply is the data-plane operator, not the webhook pod) drives
	// it to completion.
	ControlOperationRevert ControlOperation = "revert"
	// ControlOperationSkipRevert closes the revert window, making a PlanetScale
	// schema change permanent. Durable so a comment-driven skip-revert survives
	// the API process dying before the engine call lands, and so the apply owner
	// (which for a remote apply is the data-plane operator, not the webhook pod)
	// drives it to completion.
	ControlOperationSkipRevert ControlOperation = "skip_revert"
	// ControlOperationRelease releases a rollout paused after a failure under
	// on_failure 'pause', letting the held later deployments proceed (like
	// 'continue'). It is a one-way latch and is deliberately distinct from
	// 'start': 'start' resumes stopped work and carries no claim-ordering
	// clause, so it cannot release a paused rollout.
	ControlOperationRelease ControlOperation = "release"
	// ControlOperationVolume adjusts the speed/concurrency of a running schema
	// change. Durable because only the instance driving the apply holds the
	// engine state for the running schema change, and a volume RPC can land on
	// any instance sharing the route's storage. The desired level travels in
	// the request metadata (see VolumeControlRequestMetadata); the driver
	// retunes the engine at its next progress tick.
	ControlOperationVolume ControlOperation = "volume"
)

// MinVolume and MaxVolume bound the volume scale shared by every engine:
// 1 = maximum throttle (least production impact), 11 = no throttle (fastest).
const (
	MinVolume int32 = 1
	MaxVolume int32 = 11
)

// VolumeControlRequestMetadata is the JSON payload stored on a volume control
// request. The row carries the desired level so the driving instance can
// retune the engine without a synchronous exchange with the requester.
type VolumeControlRequestMetadata struct {
	Volume int32 `json:"volume"`
}

// EncodeVolumeControlRequestMetadata serializes the desired volume level for
// storage on a volume control request, rejecting out-of-range levels so an
// invalid request is refused at write time rather than discovered by the
// driver.
func EncodeVolumeControlRequestMetadata(volume int32) ([]byte, error) {
	if volume < MinVolume || volume > MaxVolume {
		return nil, fmt.Errorf("volume %d is out of range: must be between %d and %d", volume, MinVolume, MaxVolume)
	}
	data, err := json.Marshal(VolumeControlRequestMetadata{Volume: volume})
	if err != nil {
		return nil, fmt.Errorf("encode volume control request metadata for level %d: %w", volume, err)
	}
	return data, nil
}

// DecodeVolumeControlRequestMetadata parses the desired volume level from a
// volume control request's metadata, validating the shared volume range.
func DecodeVolumeControlRequestMetadata(metadata []byte) (int32, error) {
	if len(metadata) == 0 {
		return 0, fmt.Errorf("volume control request metadata is empty")
	}
	var payload VolumeControlRequestMetadata
	if err := json.Unmarshal(metadata, &payload); err != nil {
		return 0, fmt.Errorf("decode volume control request metadata: %w", err)
	}
	if payload.Volume < MinVolume || payload.Volume > MaxVolume {
		return 0, fmt.Errorf("volume %d in control request metadata is out of range: must be between %d and %d", payload.Volume, MinVolume, MaxVolume)
	}
	return payload.Volume, nil
}

// ControlRequestStatus is the durable processing status for a control request.
type ControlRequestStatus string

const (
	// ControlRequestPending means an operator driver still needs to act.
	ControlRequestPending ControlRequestStatus = "pending"
	// ControlRequestCompleted means the requested operation has been accepted.
	ControlRequestCompleted ControlRequestStatus = "completed"
	// ControlRequestFailed means the requested operation reached a terminal error.
	ControlRequestFailed ControlRequestStatus = "failed"
)

// ApplyControlRequest records durable user control intent.
// Use this when an HTTP control operation can be accepted before the engine has
// actually performed the operation.
type ApplyControlRequest struct {
	ID           int64
	ApplyID      int64
	Operation    ControlOperation
	Status       ControlRequestStatus
	RequestedBy  string
	ErrorMessage string
	Metadata     []byte
	CompletedAt  *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ReleasesPausedRollout reports whether this request latches a rollout paused
// under on_failure 'pause' open, so the held later deployments may proceed.
// release is a one-way latch: a release request that is pending or completed
// releases the rollout; a failed release attempt does not latch, so the rollout
// stays paused (fail-closed). Any non-release operation never releases.
func (r *ApplyControlRequest) ReleasesPausedRollout() bool {
	return r != nil &&
		r.Operation == ControlOperationRelease &&
		(r.Status == ControlRequestPending || r.Status == ControlRequestCompleted)
}

// ApplyOptionsFromMap converts API/proto option strings into typed storage options.
func ApplyOptionsFromMap(options map[string]string) ApplyOptions {
	opts := ApplyOptions{
		AllowUnsafe:  options["allow_unsafe"] == "true",
		Branch:       options["branch"],
		DeferCutover: options["defer_cutover"] == "true",
		DeferDeploy:  options["defer_deploy"] == "true",
		SkipRevert:   options["skip_revert"] == "true",
		Target:       options["target"],
		Rollback:     options["rollback"] == "true",
	}
	if rawVolume := options["volume"]; rawVolume != "" {
		volume, err := strconv.Atoi(rawVolume)
		if err == nil && volume >= 1 && volume <= 11 {
			opts.Volume = volume
		}
	}
	return opts
}

// Map converts typed storage options back into API/proto option strings.
func (opts ApplyOptions) Map() map[string]string {
	options := make(map[string]string)
	if opts.AllowUnsafe {
		options["allow_unsafe"] = "true"
	}
	if opts.Branch != "" {
		options["branch"] = opts.Branch
	}
	if opts.DeferCutover {
		options["defer_cutover"] = "true"
	}
	if opts.DeferDeploy {
		options["defer_deploy"] = "true"
	}
	if opts.SkipRevert {
		options["skip_revert"] = "true"
	}
	if opts.Volume > 0 {
		options["volume"] = strconv.Itoa(opts.Volume)
	}
	if opts.Target != "" {
		options["target"] = opts.Target
	}
	if opts.Rollback {
		options["rollback"] = "true"
	}
	return options
}

// ParseApplyOptions parses the JSON options into ApplyOptions.
// Returns empty options if parsing fails or options is nil/empty.
func ParseApplyOptions(data []byte) ApplyOptions {
	if len(data) == 0 {
		return ApplyOptions{}
	}
	var opts ApplyOptions
	if err := json.Unmarshal(data, &opts); err != nil {
		return ApplyOptions{}
	}
	return opts
}

// MarshalApplyOptions serializes ApplyOptions to JSON.
func MarshalApplyOptions(opts ApplyOptions) []byte {
	data, err := json.Marshal(opts)
	if err != nil {
		return []byte("{}")
	}
	return data
}

// GetOptions returns parsed options for the Apply.
func (a *Apply) GetOptions() ApplyOptions {
	return ParseApplyOptions(a.Options)
}

// SetOptions sets the options on the Apply.
func (a *Apply) SetOptions(opts ApplyOptions) {
	a.Options = MarshalApplyOptions(opts)
}

// Task represents a schema change task (individual DDL within an apply).
// For multi-table changes, one apply contains multiple tasks.
type Task struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// TaskIdentifier is the external identifier for API (e.g., "task_abc123").
	TaskIdentifier string

	// ApplyID points to applies.id.
	ApplyID int64

	// ApplyOperationID points to apply_operations.id when the task is
	// owned by a specific per-deployment operation row. Nullable for
	// backwards compatibility with rows written before the apply-create
	// path started populating this column; once the operator claim loop
	// owns task creation in the multi-deployment world, every new task
	// carries a value.
	ApplyOperationID *int64

	// PlanID points to plans.id.
	PlanID int64

	// Database is the target database name.
	Database string

	// DatabaseType is "vitess" or "mysql".
	DatabaseType string

	// Engine is the schema change engine: "spirit", "planetscale", etc.
	Engine string

	// Repository is the GitHub repository (owner/repo format).
	// Denormalized from Apply for query convenience.
	Repository string

	// PullRequest is the PR number.
	// Denormalized from Apply for query convenience.
	PullRequest int

	// Environment is "staging" or "production".
	Environment string

	// State is the current execution state.
	State string

	// ErrorMessage contains error details if state is failed.
	ErrorMessage string

	// Options contains engine-specific options as JSON.
	Options []byte

	// Attempt tracks how many times this task has been retried by operator recovery.
	Attempt int

	// Namespace is the schema name (MySQL) or keyspace (Vitess) this table belongs to.
	Namespace string

	// TableName is the table being modified (empty for multi-table atomic).
	TableName string

	// Shard is the shard this task tracks for sharded engines (e.g. "-80",
	// "80-"). Empty for unsharded engines (MySQL), which have a single shard.
	// A task is the per-(table, shard) execution record; the per-table headline
	// is aggregated across a table's shard rows at read time, never stored.
	Shard string

	// DDL is the full DDL statement.
	DDL string

	// DDLAction is "alter", "create", or "drop".
	DDLAction string

	// Progress tracking (for copy-based schema changes)
	RowsCopied      int64 // Rows copied so far
	RowsTotal       int64 // Total rows to copy
	ProgressPercent int   // 0-100
	ETASeconds      int   // Estimated seconds remaining
	// Checksum phase progress: rows verified so far and total to verify.
	// Non-zero only while the task is checksumming (verifying copied data).
	ChecksumRowsChecked int64
	ChecksumRowsTotal   int64
	CutoverAttempts     int // Number of cutover attempts for this shard

	// Execution flags
	IsInstant         bool   // True if INSTANT DDL (no copy needed)
	ReadyToComplete   bool   // Row copy done, waiting for cutover
	EngineMigrationID string // Engine-specific migration ID

	// Timestamps
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
}

// TaskFilter specifies criteria for listing tasks.
type TaskFilter struct {
	Repository       string    // Filter by repository (optional)
	PullRequest      int       // Filter by PR number, requires Repository (optional)
	IncludeCompleted bool      // Include terminal states (default: active only)
	Since            time.Time // Only tasks started after this time (optional)
}

// ApplyComment tracks a GitHub PR comment associated with an apply.
type ApplyComment struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// ApplyID points to applies.id.
	ApplyID int64

	// CommentState is the comment lifecycle state: "progress", "cutover", "summary".
	CommentState string

	// GitHubCommentID is the GitHub comment ID for editing.
	GitHubCommentID int64

	// EditCount tracks how many times this comment was edited.
	EditCount int

	// LastEditedAt is when the comment was last edited via the GitHub API.
	LastEditedAt *time.Time

	// SupersededAt is set when this comment has been retired by a newer one
	// (e.g. a stopped-summary marker consumed when the apply resumes). The row and
	// the GitHub comment are kept; SchemaBot just no longer treats it as the active
	// comment for its state. Nil means active.
	SupersededAt *time.Time

	// CreatedAt is when the comment was first posted.
	CreatedAt time.Time

	// UpdatedAt is when the comment was last edited.
	UpdatedAt time.Time
}

// ApplyLogLevel constants for log entry severity.
const (
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
)

// ApplyLogEventType constants for categorizing log entries.
const (
	LogEventStateTransition     = "state_transition"
	LogEventTaskUpdate          = "task_update"
	LogEventStopRequested       = "stop_requested"
	LogEventCancelRequested     = "cancel_requested"
	LogEventStartRequested      = "start_requested"
	LogEventReleaseRequested    = "release_requested"
	LogEventVolumeRequested     = "volume_requested"
	LogEventDeployTriggered     = "deploy_triggered"
	LogEventCutoverTriggered    = "cutover_triggered"
	LogEventSkipRevertTriggered = "skip_revert_triggered"
	LogEventRevertTriggered     = "revert_triggered"
	LogEventError               = "error"
	LogEventInfo                = "info"
	LogEventProgress            = "progress"
)

// ApplyLogSource constants for identifying the origin of log entries.
const (
	LogSourceSchemaBot = "schemabot" // Logs from SchemaBot orchestration
	LogSourceSpirit    = "spirit"    // Logs from Spirit engine
)

// ApplyLog represents a single log entry for an apply.
// Logs capture state transitions, events, and debugging info.
type ApplyLog struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// ApplyID points to applies.id.
	ApplyID int64

	// TaskID points to tasks.id (optional, for task-specific events).
	TaskID *int64

	// Level is the log level: "debug", "info", "warn", "error".
	Level string

	// EventType categorizes the log entry.
	// Examples: "state_transition", "task_update", "stop_requested".
	EventType string

	// Source identifies where the log came from: "schemabot" or "spirit".
	Source string

	// Message is the human-readable log message.
	Message string

	// OldState and NewState for state transitions (optional).
	OldState string
	NewState string

	// Metadata contains additional structured data as JSON.
	Metadata []byte

	// CreatedAt is when the log entry was created.
	CreatedAt time.Time
}

// ApplyLogFilter specifies criteria for listing apply logs.
type ApplyLogFilter struct {
	ApplyID   int64  // Required: filter by apply
	TaskID    *int64 // Optional: filter by task
	Level     string // Optional: filter by level
	EventType string // Optional: filter by event type
	Limit     int    // Optional: limit results (default 100)
}

// EngineResumeState stores opaque resume data owned by the engine package.
type EngineResumeState struct {
	ApplyOperationID int64
	MigrationContext string
	Metadata         string
}
