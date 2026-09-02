// Package apitypes defines the shared HTTP request and response types for SchemaBot's API.
// These types are used by both the server (pkg/api) and the CLI client (pkg/cmd/client).
// This package has zero dependencies — import it freely from any package.
package apitypes

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

type LogEntry struct {
	ID        int64           `json:"id"`
	ApplyID   string          `json:"apply_id"`
	TaskID    *int64          `json:"task_id,omitempty"`
	Level     string          `json:"level"`
	EventType string          `json:"event_type"`
	Source    string          `json:"source,omitempty"`
	Message   string          `json:"message"`
	OldState  string          `json:"old_state,omitempty"`
	NewState  string          `json:"new_state,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

type LogsResponse struct {
	ApplyID string      `json:"apply_id,omitempty"`
	Logs    []*LogEntry `json:"logs"`
	// Truncated reports that entries older than Logs exist and were left
	// outside the requested window. A reader that shows a full window without
	// this signal reads as a complete lifecycle, which is the wrong conclusion
	// when the interesting part scrolled past the limit.
	Truncated bool `json:"truncated,omitempty"`
}

type DeploymentLogsResponse struct {
	ApplyID    string                 `json:"apply_id"`
	Deployment string                 `json:"deployment"`
	Sources    []*DeploymentLogSource `json:"sources"`
	Errors     []*DeploymentLogError  `json:"errors"`
}

type DeploymentLogSource struct {
	ExternalID string                    `json:"external_id"`
	Operations []*LogOperationProvenance `json:"operations"`
	Logs       []*LogEntry               `json:"logs"`
	// Truncated reports that this source has entries older than Logs. Each
	// source is windowed independently, so one source can be truncated while
	// another in the same response is complete.
	Truncated bool `json:"truncated,omitempty"`
}

type LogOperationProvenance struct {
	OperationKey  string `json:"operation_key,omitempty"`
	Target        string `json:"target,omitempty"`
	OperationKind string `json:"operation_kind,omitempty"`
}

type DeploymentLogError struct {
	ExternalID string                    `json:"external_id,omitempty"`
	Target     string                    `json:"target,omitempty"`
	Operations []*LogOperationProvenance `json:"operations"`
	Code       string                    `json:"code"`
	Reason     string                    `json:"reason"`
	Message    string                    `json:"message"`
}

// =============================================================================
// Error Codes
// =============================================================================

// Error codes returned in API responses. Clients should match on these
// constants rather than parsing error_message strings or HTTP status codes.
// Use IsRetryableErrorCode to determine whether a given code is retryable.
const (
	ErrCodeInvalidRequest       = "invalid_request"        // Malformed request (missing params, bad values)
	ErrCodeNotFound             = "not_found"              // Resource doesn't exist (unknown apply ID, database)
	ErrCodeDeploymentNotFound   = "deployment_not_found"   // No tern deployment configured for database/environment
	ErrCodeEngineError          = "engine_error"           // Schema change engine failure during execution
	ErrCodeEngineErrorRetryable = "engine_error_retryable" // Schema change engine failure that may recover on retry
	ErrCodeStorageError         = "storage_error"          // Storage backend (MySQL) read/write failure
	ErrCodeEngineUnavailable    = "engine_unavailable"     // Schema change engine (Tern) unreachable or RPC error
	ErrCodeStateSyncFailed      = "state_sync_failed"      // Operation succeeded but local state sync failed
	ErrCodeActiveApplyExists    = "active_apply_exists"    // Another active apply already exists for the target
	ErrCodeSourcePolicyDenied   = "source_policy_denied"   // Source repo/path is not authorized for the database
	ErrCodeLockNotOwned         = "lock_not_owned"         // Lock release denied because the caller is not the owner
	ErrCodeRateLimited          = "rate_limited"           // Caller or target exceeded its request budget; retry after the advertised delay
)

var retryableErrorCodes = map[string]bool{
	ErrCodeEngineErrorRetryable: true,
	ErrCodeStorageError:         true,
	ErrCodeEngineUnavailable:    true,
	ErrCodeStateSyncFailed:      true,
	ErrCodeRateLimited:          true,
}

// IsRetryableErrorCode reports whether the given API error code represents a
// transient failure that clients should retry with backoff.
func IsRetryableErrorCode(code string) bool {
	return retryableErrorCodes[code]
}

// ErrorResponse is the standard error response body for non-200 HTTP responses.
// All error endpoints return this shape.
type ErrorResponse struct {
	Error     string `json:"error"`
	ErrorCode string `json:"error_code"`

	// RetryAfterSeconds is how long the client should wait before retrying,
	// set only on responses that carry a Retry-After header. It repeats the
	// header in the body because the CLI's HTTP client reads error bodies and
	// not response headers.
	RetryAfterSeconds int `json:"retry_after_seconds,omitempty"`
}

// The reasons a pull is refused for exceeding a request budget. Each names the
// budget that ran out, so a client reading only the message can tell whether it
// is being limited for its own request rate or for the load every client is
// putting on one database.
const (
	PullRateLimitCallerReason = "too many pull requests from this caller"
	PullRateLimitTargetReason = "too many pull requests for this database and environment"
)

// NewRateLimitedResponse builds the body of a 429 refusal from the budget that
// ran out and how long the caller must wait.
//
// The wait is rounded up to whole seconds, the only unit Retry-After can
// express, and is never reported as less than one: "retry in 0s" reads as an
// invitation to retry immediately, which is exactly what the budget is
// refusing. The same rounded value goes in the message and in the field, so a
// client that reads either sees one delay.
func NewRateLimitedResponse(reason string, retryAfter time.Duration) ErrorResponse {
	seconds := max(int(math.Ceil(retryAfter.Seconds())), 1)
	return ErrorResponse{
		Error:             fmt.Sprintf("%s; retry in %ds", reason, seconds),
		ErrorCode:         ErrCodeRateLimited,
		RetryAfterSeconds: seconds,
	}
}

type WebhookRedriveRequest struct {
	WindowStart string `json:"window_start"`
	WindowEnd   string `json:"window_end"`
	App         string `json:"app,omitempty"`
	Repo        string `json:"repo,omitempty"`
	PR          int    `json:"pr,omitempty"`
	MaxPages    int    `json:"max_pages"`
	DryRun      bool   `json:"dry_run,omitempty"`
	// Cursor continues a previous request's listing for one App. Each request
	// processes a bounded number of pages so it finishes well within any
	// intermediary HTTP timeout; the caller loops until next_cursor is empty.
	Cursor string `json:"cursor,omitempty"`
	// DeliveryIDs redelivers exactly these deliveries for App, skipping the
	// window listing entirely — for callers that already identified the
	// failed deliveries (for example a checks backfill classification pass).
	DeliveryIDs []int64 `json:"delivery_ids,omitempty"`
}

type WebhookRedriveResponse struct {
	Results []WebhookRedriveResult `json:"results"`
}

type WebhookRedriveResult struct {
	AppName            string `json:"app_name"`
	DryRun             bool   `json:"dry_run"`
	Fetched            int    `json:"fetched"`
	Pages              int    `json:"pages"`
	OldestFetched      string `json:"oldest_fetched,omitempty"`
	ReachedWindowStart bool   `json:"reached_window_start"`
	// HistoryExhausted is true when GitHub's retained delivery history ended
	// before the window start was reached: older deliveries no longer exist.
	HistoryExhausted bool `json:"history_exhausted,omitempty"`
	// NextCursor continues the listing in a follow-up request (with app set);
	// empty when the window is covered or history is exhausted.
	NextCursor string                    `json:"next_cursor,omitempty"`
	Selected   []WebhookRedriveSelection `json:"selected"`
	// Skipped counts in-window eligible deliveries whose detail could not be
	// resolved for repo/PR filtering; they are left out of Selected rather
	// than aborting the crawl.
	Skipped     int `json:"skipped,omitempty"`
	Redelivered int `json:"redelivered"`
	Failed      int `json:"failed"`
}

type WebhookRedriveSelection struct {
	ID          int64  `json:"id"`
	DeliveredAt string `json:"delivered_at"`
	Event       string `json:"event"`
	Action      string `json:"action"`
	Status      string `json:"status"`
	StatusCode  int    `json:"status_code"`
	Repo        string `json:"repo,omitempty"`
	PR          int    `json:"pr,omitempty"`
}

type ChecksScanRequest struct {
	Repo        string `json:"repo"`
	Environment string `json:"environment,omitempty"`
	CheckName   string `json:"check_name,omitempty"`
	// Page selects one bounded page of open PRs (1-based; 0 means the first
	// page). Each request scans a single page so it finishes well within any
	// intermediary HTTP timeout; the caller loops until next_page is 0.
	Page int `json:"page,omitempty"`
	// UpdatedSince, when set (RFC3339), scans only PRs updated at or after
	// this instant. The open-PR listing is ordered newest-updated first, so
	// the scan stops paging as soon as it crosses the cutoff — bounding an
	// incident-window sweep by the window instead of the repo's PR count.
	UpdatedSince string `json:"updated_since,omitempty"`
}

type ChecksScanResponse struct {
	Repo       string   `json:"repo"`
	CheckNames []string `json:"check_names"`
	Scanned    int      `json:"scanned"`
	NextPage   int      `json:"next_page,omitempty"`
	// ChecksDisabled reports that this repository has Check Run publishing
	// turned off (enable_checks: false), so the scan was skipped: every open
	// PR would trivially be missing a check the server refuses to create.
	// When set, no PRs were scanned and the other fields are zero.
	ChecksDisabled bool `json:"checks_disabled,omitempty"`
	// EstimatedOpenPRs is the repository's total open-PR count as GitHub
	// reports it for this page's listing — an upper bound while more pages
	// remain, exact on the final page. Recomputed every page so the caller
	// can render a progress denominator that stays honest across a long scan.
	EstimatedOpenPRs int              `json:"estimated_open_prs,omitempty"`
	Missing          []MissingCheckPR `json:"missing"`
	// Stuck lists open PRs whose expected Check Run exists but has not
	// completed. The server reports the raw status and start time; the caller
	// decides how old is old enough to call stuck, because an uncompleted
	// check is legitimate while an apply or plan is genuinely in flight.
	Stuck []StuckCheckPR `json:"stuck,omitempty"`
	// RateLimit reports the GitHub budget left on the installation that
	// served this page, so the caller can pace itself instead of starving
	// the live webhook path that shares the same budget. Nil when the rate
	// state could not be read (advisory only; the scan itself succeeded).
	RateLimit *GitHubRateLimit `json:"rate_limit,omitempty"`
}

// GitHubRateLimit is a point-in-time snapshot of a GitHub installation's
// core REST budget.
type GitHubRateLimit struct {
	Remaining int `json:"remaining"`
	Limit     int `json:"limit"`
	// ResetAt is RFC3339; when the budget replenishes.
	ResetAt string `json:"reset_at"`
}

// ChecksReposResponse lists the repositories declared in the server's repos
// config — the inventory a fleet-wide scan iterates.
type ChecksReposResponse struct {
	// Repos are the declared repositories with Check Run publishing enabled —
	// the ones a checks scan or backfill can act on.
	Repos []string `json:"repos"`
	// Disabled are declared repositories with Check Run publishing turned off
	// (enable_checks: false). A scan of these would report every open PR as
	// missing a check that the server refuses to create; callers skip them
	// and report the skip to the operator.
	Disabled []string `json:"disabled,omitempty"`
}

// ChecksSynthesizeRequest asks the server to recreate missing Check Runs for
// specific PRs by replaying the auto-plan flow, as if the check-creating
// webhook delivery had arrived. Used for PRs with no delivery to redrive
// (for example PRs opened before check enablement).
type ChecksSynthesizeRequest struct {
	Repo string `json:"repo"`
	PRs  []int  `json:"prs"`
}

type ChecksSynthesizeResponse struct {
	Repo    string                   `json:"repo"`
	Results []ChecksSynthesizeResult `json:"results"`
	// RateLimit mirrors ChecksScanResponse.RateLimit: the installation's
	// remaining GitHub budget after this batch, for caller-side pacing.
	RateLimit *GitHubRateLimit `json:"rate_limit,omitempty"`
}

type ChecksSynthesizeResult struct {
	PR      int    `json:"pr"`
	Outcome string `json:"outcome"`
	Error   string `json:"error,omitempty"`
}

type MissingCheckPR struct {
	Number       int      `json:"number"`
	URL          string   `json:"url"`
	Title        string   `json:"title"`
	HeadSHA      string   `json:"head_sha"`
	HeadRef      string   `json:"head_ref"`
	MissingNames []string `json:"missing_check_names"`
	// UntrustedConflictNames are missing check names for which a same-named
	// Check Run already exists but was created by an untrusted app. Backfill
	// still recreates the trusted check, but the operator likely also needs to
	// remove/rename the conflicting check or adjust the trusted-app config.
	UntrustedConflictNames []string `json:"untrusted_conflict_check_names,omitempty"`
}

// StuckCheckPR is an open PR carrying at least one expected SchemaBot Check
// Run that exists but has not reached a conclusion.
type StuckCheckPR struct {
	Number  int                  `json:"number"`
	URL     string               `json:"url"`
	Title   string               `json:"title"`
	HeadSHA string               `json:"head_sha"`
	HeadRef string               `json:"head_ref"`
	Checks  []IncompleteCheckRun `json:"checks"`
}

// IncompleteCheckRun describes one Check Run that exists on the PR head but
// has not completed.
type IncompleteCheckRun struct {
	Name       string `json:"name"`
	CheckRunID int64  `json:"check_run_id"`
	Status     string `json:"status"`
	// StartedAt is RFC3339; empty when GitHub did not report a start time.
	StartedAt string `json:"started_at,omitempty"`
}

// =============================================================================
// Request Types
// =============================================================================

// SchemaFiles contains the schema files for a namespace (schema name for MySQL,
// keyspace for Vitess). This is a lightweight equivalent of ternv1.SchemaFiles
// that avoids pulling in proto dependencies.
type SchemaFiles struct {
	Files map[string]string `json:"files,omitempty"`
}

// PulledNamespace contains live schema content for a namespace (schema name for
// MySQL, keyspace for Vitess). It intentionally describes database objects, not
// repository filenames; clients decide how to materialize tables and artifacts.
type PulledNamespace struct {
	Tables           map[string]string        `json:"tables,omitempty"`
	Artifacts        map[string]string        `json:"artifacts,omitempty"`
	NamespaceCatalog *NamespaceCatalog        `json:"namespace_catalog,omitempty"`
	TableCatalog     map[string]*TableCatalog `json:"table_catalog,omitempty"`
	// Lint holds the schema lint violations for this namespace's tables,
	// populated only when the pull request asked for linting. A namespace
	// with no violations serializes as an explicit empty list so a clean
	// audit is distinguishable from lint not being requested (omitted).
	Lint []*LintViolationResponse `json:"lint,omitzero"`
}

// NamespaceCatalog contains structured metadata for a pulled namespace.
type NamespaceCatalog struct {
	Name       string `json:"name"`
	Engine     string `json:"engine"`
	TableCount int32  `json:"table_count"`
}

// TableCatalog contains structured metadata for a pulled table or view.
type TableCatalog struct {
	Name    string           `json:"name"`
	Kind    string           `json:"kind"`
	Comment string           `json:"comment,omitempty"`
	Columns []*ColumnCatalog `json:"columns,omitempty"`
	Indexes []*IndexCatalog  `json:"indexes,omitempty"`
	// EstimatedRowCount and DataSizeBytes are engine-maintained estimates
	// (from information_schema for MySQL) and may be stale until statistics
	// are refreshed; they are not exact counts.
	EstimatedRowCount int64                `json:"estimated_row_count,omitempty"`
	DataSizeBytes     int64                `json:"data_size_bytes,omitempty"`
	ForeignKeys       []*ForeignKeyCatalog `json:"foreign_keys,omitempty"`
}

// ColumnCatalog contains structured metadata for a pulled table column.
type ColumnCatalog struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	DefaultValue  string `json:"default_value,omitempty"`
	Comment       string `json:"comment,omitempty"`
	AutoIncrement bool   `json:"auto_increment,omitempty"`
	Generated     bool   `json:"generated,omitempty"`
}

// IndexCatalog contains structured metadata for a pulled table index.
type IndexCatalog struct {
	Name    string   `json:"name"`
	Primary bool     `json:"primary,omitempty"`
	Unique  bool     `json:"unique,omitempty"`
	Parts   []string `json:"parts,omitempty"`
}

// ForeignKeyCatalog contains structured metadata for a foreign-key constraint.
type ForeignKeyCatalog struct {
	Name              string   `json:"name"`
	Columns           []string `json:"columns,omitempty"`
	ReferencedTable   string   `json:"referenced_table"`
	ReferencedColumns []string `json:"referenced_columns,omitempty"`
	OnUpdate          string   `json:"on_update,omitempty"`
	OnDelete          string   `json:"on_delete,omitempty"`
}

// PullSchemaRequest is the HTTP request body for POST /api/pull.
type PullSchemaRequest struct {
	Database      string   `json:"database"`
	Environment   string   `json:"environment"`
	Type          string   `json:"type"`
	Namespaces    []string `json:"namespaces,omitempty"`
	CatalogDetail string   `json:"catalog_detail,omitempty"`
	// Lint runs the schema linters over every pulled table and attaches the
	// violations to each namespace, so a caller can audit a database's lint
	// debt without planning a change. Off by default.
	Lint bool `json:"lint,omitempty"`
}

// PullSchemaResponse is the HTTP response body for POST /api/pull.
type PullSchemaResponse struct {
	Database    string                      `json:"database"`
	Type        string                      `json:"type"`
	Environment string                      `json:"environment"`
	Namespaces  map[string]*PulledNamespace `json:"namespaces"`
	TableCount  int32                       `json:"table_count"`
}

// DatabaseListResponse is the HTTP response body for GET /api/databases.
type DatabaseListResponse struct {
	Databases []*DatabaseResponse `json:"databases"`
}

// DatabaseResponse describes one server-side database without
// exposing connection strings, opaque execution targets, or endpoint addresses.
type DatabaseResponse struct {
	Database     string                         `json:"database"`
	Type         string                         `json:"type"`
	Environments []*DatabaseEnvironmentResponse `json:"environments"`
}

// DatabaseEnvironmentResponse describes one configured database environment
// without exposing connection strings, opaque execution targets, or endpoints.
type DatabaseEnvironmentResponse struct {
	Environment string   `json:"environment"`
	Deployments []string `json:"deployments,omitempty"`
}

// PlanRequest is the HTTP request body for POST /api/plan.
type PlanRequest struct {
	Database    string                  `json:"database"`
	Environment string                  `json:"environment"`
	Type        string                  `json:"type"`
	SchemaFiles map[string]*SchemaFiles `json:"schema_files"`
	Repository  string                  `json:"repository,omitempty"`
	PullRequest *int32                  `json:"pull_request,omitempty"`
	// HeadSHA is the PR HEAD SHA at the time the schema files were discovered.
	// Persisted on the plan record and used at apply-confirm time to detect the
	// cross-delivery race where HEAD advances between plan and confirm.
	// Optional — absent for non-webhook callers (e.g. CLI plan invocations without a PR).
	HeadSHA *string `json:"head_sha,omitempty"`
	// IgnoredNamespaces lists the namespaces the caller removed from
	// SchemaFiles per the config's ignore_namespaces, resolved for the
	// environment. The data plane refuses engine shapes that diff the whole
	// target as one unit (a database-scoped MySQL DSN), where a withheld
	// namespace's live tables would otherwise be planned as drops.
	IgnoredNamespaces []string `json:"ignored_namespaces,omitempty"`
	// GroupedExecution reports whether an apply of this plan will hand the
	// engine every ALTER at once or one table at a time. Engines predicting what
	// an apply will do to unfinished work already on the target need the
	// grouping the apply will actually run under.
	GroupedExecution bool `json:"grouped_execution,omitempty"`
}

// ApplyRequest is the HTTP request body for POST /api/apply.
type ApplyRequest struct {
	PlanID      string            `json:"plan_id"`
	Environment string            `json:"environment"`
	Caller      string            `json:"caller,omitempty"`
	Options     map[string]string `json:"options,omitempty"`
}

// ControlRequest is the HTTP request body for control operations
// (stop, start, cutover, revert, skip-revert).
type ControlRequest struct {
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id"`
	Caller      string `json:"caller,omitempty"`
}

// =============================================================================
// Response Types
// =============================================================================

// PlanResponse is the HTTP response for POST /api/plan.
type PlanResponse struct {
	PlanID       string `json:"plan_id"`
	Database     string `json:"database,omitempty"`
	DatabaseType string `json:"database_type,omitempty"`
	Environment  string `json:"environment,omitempty"`
	// Deployment is the primary deployment this plan was created against
	// (rollout index 0 at plan time). The review-time drift rollup carries it
	// forward so it can verify the plan's baseline still maps to the primary at
	// rollup time, rather than trusting that current config re-resolves the same
	// primary.
	Deployment  string                   `json:"deployment,omitempty"`
	Engine      string                   `json:"engine"`
	Changes     []*SchemaChangeResponse  `json:"changes"`
	LintResults []*LintViolationResponse `json:"lint_violations"`
	Errors      []string                 `json:"errors"`
	// Shards carries the per-shard plan for a sharded engine: each changing shard
	// and the changes it needs. The namespace-level Changes above collapse a
	// keyspace to one entry, so a keyspace whose shards diverge is represented
	// faithfully only here. Empty for non-sharded plans.
	Shards []*ShardPlanResponse `json:"shards,omitempty"`
	// ExistingCopies carries the unfinished copies already on the target that
	// applying this plan will adopt or discard, one entry per namespace holding
	// any. Empty when the target is clean, which is the ordinary case.
	ExistingCopies []*ExistingCopyResponse `json:"existing_copies,omitempty"`
}

// Dispositions an ExistingCopyResponse can carry. These mirror the engine's
// own vocabulary on the wire; this package holds its own copy because it is
// dependency-free by design. A test pins the two together.
const (
	ExistingCopyAdopt   = "adopt"
	ExistingCopyDiscard = "discard"
)

// ExistingCopyResponse is an unfinished copy sitting on the target and what
// applying the plan will do to it: adopt it and resume, or discard it and copy
// again from the start.
type ExistingCopyResponse struct {
	Namespace   string   `json:"namespace,omitempty"`
	Disposition string   `json:"disposition"`
	Reason      string   `json:"reason,omitempty"`
	Tables      []string `json:"tables,omitempty"`
	AgeSeconds  int64    `json:"age_seconds,omitempty"`
	Statement   string   `json:"statement,omitempty"`
	// Running reports that the copy is still being made right now rather than
	// left behind by an apply that is over. It changes both what applying does
	// — joins the copy instead of resuming it — and what AgeSeconds means, which
	// for a running copy is the interval between checkpoints rather than how
	// stale the work is.
	//
	// It reports stored state rather than a live probe, so a task row left in
	// flight by a crashed server reads as running until recovery clears it.
	// Treat it as how to describe the copy, never as proof that work is live:
	// what applying does to the copy is carried by Disposition.
	Running bool `json:"running,omitempty"`
}

// ShardPlanResponse is one changing shard's plan: the keyspace it belongs to and
// the table changes that shard needs.
type ShardPlanResponse struct {
	Namespace string                 `json:"namespace,omitempty"`
	Shard     string                 `json:"shard"`
	Changes   []*TableChangeResponse `json:"changes,omitempty"`
}

// PlanSummaryResponse is one stored plan in the GET /api/plans listing: its
// provenance plus change counts derived from the stored plan data, without
// the plan content itself.
type PlanSummaryResponse struct {
	PlanID       string `json:"plan_id"`
	Database     string `json:"database"`
	DatabaseType string `json:"database_type"`
	Deployment   string `json:"deployment,omitempty"`
	Environment  string `json:"environment"`
	// Repository and PullRequest identify the PR the plan was generated for.
	// Both empty means an ad-hoc plan, such as a CLI plan invocation without
	// a PR.
	Repository  string    `json:"repository,omitempty"`
	PullRequest int       `json:"pull_request,omitempty"`
	HeadSHA     string    `json:"head_sha,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	// ChangeCounts maps a change operation ("create", "alter", "drop", ...) to
	// how many table changes of that operation the plan carries across every
	// namespace. Empty for a no-change plan.
	ChangeCounts map[string]int `json:"change_counts,omitempty"`
	// UnsafeCount is how many of those table changes the planner marked
	// unsafe.
	UnsafeCount int `json:"unsafe_count,omitempty"`
	// BlockedCount is how many of those table changes the engine will
	// deterministically refuse (execution mode "blocked").
	BlockedCount int `json:"blocked_count,omitempty"`
	// VSchemaChangeCount is how many namespaces carry a VSchema change.
	VSchemaChangeCount int `json:"vschema_change_count,omitempty"`
}

// PlansResponse is the HTTP response for GET /api/plans.
type PlansResponse struct {
	Limit    int  `json:"limit"`
	MaxLimit int  `json:"max_limit"`
	HasMore  bool `json:"has_more"`
	// Last echoes the requested creation window, when one was given.
	Last  string                 `json:"last,omitempty"`
	Plans []*PlanSummaryResponse `json:"plans"`
}

// StoredPlanResponse is the HTTP response for GET /api/plans/{plan_identifier}:
// the summary plus the full stored plan content, reconstructed in the same
// shape POST /api/plan returns so plan rendering is shared. Lint results and
// errors are not persisted with a plan, so Plan carries changes only —
// their absence means "not stored", not "clean".
type StoredPlanResponse struct {
	PlanSummaryResponse
	SchemaPath string        `json:"schema_path,omitempty"`
	Target     string        `json:"target,omitempty"`
	Plan       *PlanResponse `json:"plan"`
}

// HasErrors returns true if any lint result has error severity.
func (r *PlanResponse) HasErrors() bool {
	for _, w := range r.LintResults {
		if w.Severity == "error" {
			return true
		}
	}
	return false
}

// UnsafeChange represents a table change that is potentially destructive.
type UnsafeChange struct {
	Table      string
	Reason     string
	DDL        string
	ChangeType string
}

// UnsafeChanges returns all changes marked as unsafe across all namespaces:
// unsafe table changes, VSchema removals, and in-place vindex mutations. DROP
// table changes are treated as unsafe even when an engine omits IsUnsafe, so
// destructive table deletion fails closed.
func (r *PlanResponse) UnsafeChanges() []UnsafeChange {
	if r == nil {
		return nil
	}
	var result []UnsafeChange
	for _, sc := range r.Changes {
		if sc == nil {
			continue
		}
		for _, t := range sc.TableChanges {
			if unsafeChange, ok := t.UnsafeChange(); ok {
				result = append(result, unsafeChange)
			}
		}
		result = append(result, sc.VSchemaUnsafeChanges()...)
	}
	return result
}

// HasBlockedChanges reports whether any planned change carries the blocked
// execution-mode verdict, across namespace-level and per-shard changes. A
// blocked change guarantees the apply fails, so gates use this to reject the
// apply before it starts.
func (r *PlanResponse) HasBlockedChanges() bool {
	if r == nil {
		return false
	}
	for _, t := range r.FlatTables() {
		if t.EngineBlocked() {
			return true
		}
	}
	for _, sp := range r.Shards {
		if sp == nil {
			continue
		}
		for _, t := range sp.Changes {
			if t.EngineBlocked() {
				return true
			}
		}
	}
	return false
}

// DiscardedCopies returns the unfinished copies on the target that applying
// this plan will throw away and re-copy from the start.
//
// A copy whose disposition this build does not recognize counts as discarded:
// the caller uses this to decide whether an operator must confirm before work
// already done is destroyed, and an unrecognized verdict is not a reason to
// skip that confirmation.
func (r *PlanResponse) DiscardedCopies() []*ExistingCopyResponse {
	if r == nil {
		return nil
	}
	var result []*ExistingCopyResponse
	for _, c := range r.ExistingCopies {
		if c == nil {
			continue
		}
		if c.Disposition == ExistingCopyAdopt {
			continue
		}
		result = append(result, c)
	}
	return result
}

// DirectChanges returns all table changes the planner routed to direct
// execution, across namespace-level and per-shard changes.
func (r *PlanResponse) DirectChanges() []*TableChangeResponse {
	if r == nil {
		return nil
	}
	var result []*TableChangeResponse
	for _, t := range r.FlatTables() {
		if t.DirectExecution() {
			result = append(result, t)
		}
	}
	for _, sp := range r.Shards {
		if sp == nil {
			continue
		}
		for _, t := range sp.Changes {
			if t.DirectExecution() {
				result = append(result, t)
			}
		}
	}
	return result
}

// AllChangesDirect reports whether every planned change is a direct-execution
// change (and at least one exists). Options that only affect engine-driven
// statements — like a deferred cutover — have nothing to act on in such a
// plan, so their commands are rejected rather than silently ignored.
func (r *PlanResponse) AllChangesDirect() bool {
	if r == nil {
		return false
	}
	total := 0
	for _, sc := range r.Changes {
		if sc == nil {
			continue
		}
		if sc.HasVSchemaChange() {
			return false
		}
		total += len(sc.TableChanges)
		for _, t := range sc.TableChanges {
			if !t.DirectExecution() {
				return false
			}
		}
	}
	for _, sp := range r.Shards {
		if sp == nil {
			continue
		}
		total += len(sp.Changes)
		for _, t := range sp.Changes {
			if !t.DirectExecution() {
				return false
			}
		}
	}
	return total > 0
}

// LintWarnings returns lint results with warning severity.
func (r *PlanResponse) LintWarnings() []LintViolationResponse {
	var result []LintViolationResponse
	for _, w := range r.LintResults {
		if w.Severity == "warning" {
			result = append(result, *w)
		}
	}
	return result
}

// LintInfos returns lint results with info severity.
func (r *PlanResponse) LintInfos() []LintViolationResponse {
	var result []LintViolationResponse
	for _, w := range r.LintResults {
		if w.Severity == "info" {
			result = append(result, *w)
		}
	}
	return result
}

// LintNonErrors returns lint results that don't block the apply (warning + info).
func (r *PlanResponse) LintNonErrors() []LintViolationResponse {
	return append(r.LintWarnings(), r.LintInfos()...)
}

// LintErrors returns lint results with error severity.
func (r *PlanResponse) LintErrors() []LintViolationResponse {
	var result []LintViolationResponse
	for _, w := range r.LintResults {
		if w.Severity == "error" {
			result = append(result, *w)
		}
	}
	return result
}

// FlatTables returns a flat list of all table changes across all namespaces.
func (r *PlanResponse) FlatTables() []*TableChangeResponse {
	var tables []*TableChangeResponse
	for _, sc := range r.Changes {
		if sc == nil {
			continue
		}
		tables = append(tables, sc.TableChanges...)
	}
	return tables
}

// HasChanges reports whether the plan carries any work an apply would execute:
// table DDL in any namespace, or a VSchema update. Gates that decide whether a
// plan is actionable must use this rather than counting table changes alone —
// a VSchema-only plan has zero table changes but still requires an apply.
func (r *PlanResponse) HasChanges() bool {
	for _, sc := range r.Changes {
		if sc == nil {
			continue
		}
		if len(sc.TableChanges) > 0 || sc.HasVSchemaChange() {
			return true
		}
	}
	return false
}

// SchemaChangeResponse groups changes for a single namespace.
type SchemaChangeResponse struct {
	Namespace    string                 `json:"namespace"`
	TableChanges []*TableChangeResponse `json:"table_changes,omitempty"`
	Metadata     map[string]string      `json:"metadata,omitempty"` // Engine-specific data (e.g., "vschema" → diff)
}

// TableChangeResponse represents a DDL change in the HTTP response.
type TableChangeResponse struct {
	TableName    string `json:"table_name"`
	Namespace    string `json:"namespace,omitempty"`
	DDL          string `json:"ddl"`
	ChangeType   string `json:"change_type"`
	IsUnsafe     bool   `json:"is_unsafe,omitempty"`
	UnsafeReason string `json:"unsafe_reason,omitempty"`
	// ExecutionMode is the planner's execution-mode verdict. Empty means the
	// engine's default path; "blocked" means the engine deterministically
	// refuses the statement and the apply will fail; "direct" means the
	// database's direct execution policy routes the refused statement to
	// native DDL on the target instead.
	ExecutionMode string `json:"execution_mode,omitempty"`
	// ModeReason is the engine's reason for any non-empty ExecutionMode
	// verdict.
	ModeReason string `json:"mode_reason,omitempty"`
}

// Execution-mode verdicts a planner records on a table change. These mirror
// the engine-side constants (pkg/engine); apitypes keeps its own copies so
// this package stays dependency-free.
const (
	executionModeBlocked = "blocked"
	executionModeDirect  = "direct"
)

// GetTableName implements ddl.TableWithName for filtering Spirit internal tables.
func (t *TableChangeResponse) GetTableName() string { return t.TableName }

// EngineBlocked reports whether the planner's execution-mode verdict says the
// engine deterministically refuses this change: an apply will fail on it.
func (t *TableChangeResponse) EngineBlocked() bool {
	return t != nil && strings.EqualFold(t.ExecutionMode, executionModeBlocked)
}

// DirectExecution reports whether the planner's execution-mode verdict routes
// this change to direct execution: it runs as native MySQL DDL — synchronous,
// blocking writes to the table while it runs, and not revertible.
func (t *TableChangeResponse) DirectExecution() bool {
	return t != nil && strings.EqualFold(t.ExecutionMode, executionModeDirect)
}

// DropsTable reports whether this change removes the table from the target.
func (t *TableChangeResponse) DropsTable() bool {
	return t != nil && strings.EqualFold(t.ChangeType, "drop")
}

// UnsafeChange returns the unsafe-change view for table changes that require
// explicit operator opt-in. Engines should mark unsafe table changes directly;
// the drop fallback keeps table deletion fail-closed if an engine omits that
// metadata.
func (t *TableChangeResponse) UnsafeChange() (UnsafeChange, bool) {
	if t == nil {
		return UnsafeChange{}, false
	}
	if !t.IsUnsafe && !t.DropsTable() {
		return UnsafeChange{}, false
	}
	reason := t.UnsafeReason
	if reason == "" && t.DropsTable() {
		reason = "DROP TABLE removes all data"
	}
	return UnsafeChange{
		Table:      t.TableName,
		Reason:     reason,
		DDL:        t.DDL,
		ChangeType: t.ChangeType,
	}, true
}

// LintViolationResponse represents a lint violation in the HTTP response.
type LintViolationResponse struct {
	Message  string `json:"message"`
	Table    string `json:"table,omitempty"`
	Column   string `json:"column,omitempty"`
	Linter   string `json:"linter,omitempty"`
	Severity string `json:"severity,omitempty"` // "error", "warning", or "info"
	FixType  string `json:"fix_type,omitempty"`
}

// ApplyResponse is the HTTP response for POST /api/apply.
type ApplyResponse struct {
	Accepted     bool   `json:"accepted"`
	ApplyID      string `json:"apply_id,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// ControlResponse is the HTTP response for simple control operations
// (cutover, revert, skip-revert) that return accepted + optional error.
type ControlResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorMessage string `json:"error_message,omitempty"`
	Status       string `json:"status,omitempty"`
}

const (
	ControlStatusAlreadyInProgress = "already_in_progress"
	ControlStatusAlreadyRequested  = "already_requested"
)

// StopResponse is the HTTP response for POST /api/stop.
type StopResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorMessage string `json:"error_message,omitempty"`
	StoppedCount int64  `json:"stopped_count"`
	SkippedCount int64  `json:"skipped_count"`
	Status       string `json:"status,omitempty"`
}

// CancelResponse is the HTTP response for POST /api/cancel.
type CancelResponse struct {
	Accepted       bool   `json:"accepted"`
	ErrorMessage   string `json:"error_message,omitempty"`
	CancelledCount int64  `json:"cancelled_count"`
	SkippedCount   int64  `json:"skipped_count"`
	Status         string `json:"status,omitempty"`
}

// StartResponse is the HTTP response for POST /api/start.
type StartResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	SkippedCount int64  `json:"skipped_count"`
	Status       string `json:"status,omitempty"`
	StartedCount int64  `json:"started_count"`
}

// ReleaseResponse is the HTTP response for POST /api/release.
type ReleaseResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorMessage string `json:"error_message,omitempty"`
	Status       string `json:"status,omitempty"`
}

// ProgressResponse is the HTTP response for GET /api/progress/apply/{apply_id}.
type ProgressResponse struct {
	State        string `json:"state"`
	Engine       string `json:"engine"`
	ApplyID      string `json:"apply_id,omitempty"`
	Database     string `json:"database,omitempty"`      // Included in apply-id lookups
	DatabaseType string `json:"database_type,omitempty"` // Included in apply-id lookups
	Environment  string `json:"environment,omitempty"`   // Included in apply-id lookups
	Caller       string `json:"caller,omitempty"`        // Included in apply-id lookups
	PullRequest  string `json:"pull_request,omitempty"`  // PR URL (blank for CLI context)
	StartedAt    string `json:"started_at,omitempty"`
	CompletedAt  string `json:"completed_at,omitempty"`
	// Operations carries per-deployment operation rows for multi-deployment applies.
	// Empty for single-deployment applies.
	Operations   []*ProgressOperationResponse `json:"operations,omitempty"`
	Tables       []*TableProgressResponse     `json:"tables,omitempty"`
	ErrorCode    string                       `json:"error_code,omitempty"`
	ErrorMessage string                       `json:"error_message,omitempty"`
	Summary      string                       `json:"summary,omitempty"`  // Combined status with ETA
	Options      map[string]string            `json:"options,omitempty"`  // Apply options (defer_cutover, skip_revert, etc.)
	Metadata     map[string]string            `json:"metadata,omitempty"` // Engine-specific data
	// Released is true when an operator has released a paused rollout open via a
	// release control request, so a deployment that failed under
	// on_failure=pause no longer holds later deployments — the rollout proceeds
	// like continue. Apply-level: it applies to every operation of the apply.
	Released bool `json:"released,omitempty"`
}

// ProgressOperationResponse represents progress for one deployment operation.
type ProgressOperationResponse struct {
	Deployment string `json:"deployment"`
	// OperationKey disambiguates multiple execution operations in the same
	// apply and deployment, correlating this row with the stored
	// apply_operation and its data-plane work. Empty is the legacy
	// single-operation key.
	OperationKey string `json:"operation_key,omitempty"`
	// ExternalID is the remote data plane's stable apply identifier.
	ExternalID string `json:"external_id,omitempty"`
	// ExternalOperationID is the remote data plane's numeric operation row ID.
	ExternalOperationID string `json:"external_operation_id,omitempty"`
	OperationKind       string `json:"operation_kind,omitempty"`
	Target              string `json:"target,omitempty"`
	State               string `json:"state"`
	// CutoverPolicy is the rollout boundary policy for this deployment operation.
	CutoverPolicy string `json:"cutover_policy,omitempty"`
	// OnFailure is the rollout failure policy for this deployment operation.
	OnFailure    string `json:"on_failure,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	StartedAt    string `json:"started_at,omitempty"`
	CompletedAt  string `json:"completed_at,omitempty"`
}

// TableProgressResponse represents progress for a single table.
type TableProgressResponse struct {
	TableName string `json:"table_name"`
	DDL       string `json:"ddl"`
	// Deployment attributes this table/task to a deployment in a multi-deployment apply.
	// Empty for single-deployment applies.
	Deployment      string `json:"deployment,omitempty"`
	Keyspace        string `json:"keyspace,omitempty"`
	ChangeType      string `json:"change_type,omitempty"` // create, alter, drop
	Status          string `json:"status"`
	RowsCopied      int64  `json:"rows_copied"`
	RowsTotal       int64  `json:"rows_total"`
	PercentComplete int32  `json:"percent_complete"`
	ETASeconds      int64  `json:"eta_seconds,omitempty"`
	// Checksum phase progress: rows verified so far and total to verify.
	// Non-zero only while the table is checksumming (verifying copied data).
	ChecksumRowsChecked int64 `json:"checksum_rows_checked,omitempty"`
	ChecksumRowsTotal   int64 `json:"checksum_rows_total,omitempty"`
	// The engine's throttler is pausing this table's active phase (row copy or
	// checksum verify). ThrottleReason names the signal for display and is
	// empty when Throttled is false.
	Throttled      bool                     `json:"throttled,omitempty"`
	ThrottleReason string                   `json:"throttle_reason,omitempty"`
	IsInstant      bool                     `json:"is_instant,omitempty"`
	ProgressDetail string                   `json:"progress_detail,omitempty"`
	TaskID         string                   `json:"task_id,omitempty"`
	StartedAt      string                   `json:"started_at,omitempty"`
	CompletedAt    string                   `json:"completed_at,omitempty"`
	Shards         []*ShardProgressResponse `json:"shards,omitempty"`
}

// ShardProgressResponse contains per-shard progress for Vitess schema changes.
type ShardProgressResponse struct {
	Shard           string `json:"shard"`
	Status          string `json:"status"`
	RowsCopied      int64  `json:"rows_copied"`
	RowsTotal       int64  `json:"rows_total"`
	ETASeconds      int64  `json:"eta_seconds,omitempty"`
	PercentComplete int32  `json:"percent_complete"`
	CutoverAttempts int32  `json:"cutover_attempts,omitempty"`
}

// GetTableName implements ddl.TableWithName for filtering Spirit internal tables.
func (t *TableProgressResponse) GetTableName() string { return t.TableName }

// ApplyHistoryResponse represents a single apply in the history.
type ApplyHistoryResponse struct {
	ApplyID     string `json:"apply_id"`
	Caller      string `json:"caller"`
	CompletedAt string `json:"completed_at,omitempty"`
	Engine      string `json:"engine"`
	Environment string `json:"environment"`
	Error       string `json:"error,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	State       string `json:"state"`
}

// DatabaseHistoryResponse is the response for GET /api/history/{database}.
type DatabaseHistoryResponse struct {
	Database string                  `json:"database"`
	Applies  []*ApplyHistoryResponse `json:"applies"`
}

// ActiveApplyResponse represents a schema change in the status list.
type ActiveApplyResponse struct {
	ApplyID string `json:"apply_id"`
	// ExternalID is the remote data plane's stable apply identifier.
	ExternalID string `json:"external_id,omitempty"`
	// ExternalOperationID is the remote data plane's numeric operation row ID.
	ExternalOperationID string `json:"external_operation_id,omitempty"`
	Database            string `json:"database"`
	Environment         string `json:"environment"`
	Deployment          string `json:"deployment,omitempty"`
	State               string `json:"state"`
	Engine              string `json:"engine"`
	Caller              string `json:"caller"`
	ErrorMessage        string `json:"error_message,omitempty"`
	StartedAt           string `json:"started_at,omitempty"`
	CompletedAt         string `json:"completed_at,omitempty"`
	UpdatedAt           string `json:"updated_at"`
}

// StatusResponse is the HTTP response for GET /api/status.
type StatusResponse struct {
	ActiveCount  int  `json:"active_count"`
	Limit        int  `json:"limit,omitempty"`
	MaxLimit     int  `json:"max_limit,omitempty"`
	HasMore      bool `json:"has_more,omitempty"`
	FailuresOnly bool `json:"failures_only,omitempty"`
	// Last echoes the window bounding the list to applies updated within it;
	// empty means the list is bounded by limit alone.
	Last string `json:"last,omitempty"`
	// State echoes the canonical form of the state filter restricting the
	// list; empty means no state filter.
	State string `json:"state,omitempty"`
	// StateCounts tallies every apply matching the request's filters by state,
	// unbounded by limit — the applies list may be a truncated page, but these
	// counts never are.
	StateCounts map[string]int         `json:"state_counts,omitempty"`
	Applies     []*ActiveApplyResponse `json:"applies"`
}
