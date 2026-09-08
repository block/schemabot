// Package client provides HTTP client utilities for the SchemaBot CLI.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// ResolveEndpoint returns the endpoint to use, checking in order:
// 1. Explicit flag value
// 2. SCHEMABOT_ENDPOINT environment variable
// 3. Config file value (if provided)
func ResolveEndpoint(flag string, configEndpoint ...string) string {
	if flag != "" {
		return strings.TrimSuffix(flag, "/")
	}
	if env := os.Getenv("SCHEMABOT_ENDPOINT"); env != "" {
		return strings.TrimSuffix(env, "/")
	}
	if len(configEndpoint) > 0 && configEndpoint[0] != "" {
		return strings.TrimSuffix(configEndpoint[0], "/")
	}
	return ""
}

// GetEnvironments fetches the list of environments for a database from the API.
func GetEnvironments(endpoint, database string) ([]string, error) {
	var result struct {
		Environments []string `json:"environments"`
	}
	path := fmt.Sprintf("/api/databases/%s/environments", url.PathEscape(database))
	if err := doGetInto(endpoint, path, &result); err != nil {
		return nil, err
	}
	return result.Environments, nil
}

// ListDatabasesOptions controls database list request fields.
type ListDatabasesOptions struct {
	Type string
	// Name keeps only databases whose name contains it, case-insensitively.
	Name string
}

// ListDatabases fetches the configured databases known to the server.
func ListDatabases(endpoint string, opts ListDatabasesOptions) (*apitypes.DatabaseListResponse, error) {
	requestPath := "/api/databases"
	values := url.Values{}
	if opts.Type != "" {
		values.Set("type", opts.Type)
	}
	if opts.Name != "" {
		values.Set("name", opts.Name)
	}
	if encoded := values.Encode(); encoded != "" {
		requestPath += "?" + encoded
	}
	var result apitypes.DatabaseListResponse
	if err := doGetInto(endpoint, requestPath, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func RedriveWebhooks(ctx context.Context, endpoint string, req apitypes.WebhookRedriveRequest) (*apitypes.WebhookRedriveResponse, error) {
	var result apitypes.WebhookRedriveResponse
	if err := doSlowPostIntoCtx(ctx, endpoint, "/api/webhooks/redrive", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func ChecksScan(ctx context.Context, endpoint string, req apitypes.ChecksScanRequest) (*apitypes.ChecksScanResponse, error) {
	var result apitypes.ChecksScanResponse
	if err := doSlowPostIntoCtx(ctx, endpoint, "/api/checks/scan", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func ChecksSynthesize(ctx context.Context, endpoint string, req apitypes.ChecksSynthesizeRequest) (*apitypes.ChecksSynthesizeResponse, error) {
	var result apitypes.ChecksSynthesizeResponse
	if err := doSlowPostIntoCtx(ctx, endpoint, "/api/checks/synthesize", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ChecksRepos lists the repositories declared in the server's repos config —
// the inventory a fleet-wide checks scan iterates.
func ChecksRepos(ctx context.Context, endpoint string) (*apitypes.ChecksReposResponse, error) {
	var result apitypes.ChecksReposResponse
	if err := doSlowPostIntoCtx(ctx, endpoint, "/api/checks/repos", struct{}{}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// PullSchemaOptions controls optional live schema pull request fields.
type PullSchemaOptions struct {
	Namespaces    []string
	CatalogDetail string
	Lint          bool
}

// CallPullSchemaAPI fetches live schema files for a database/environment pair.
func CallPullSchemaAPI(endpoint, database, dbType, environment string, namespaces ...string) (*apitypes.PullSchemaResponse, error) {
	return CallPullSchemaAPIWithOptions(endpoint, database, dbType, environment, PullSchemaOptions{Namespaces: namespaces})
}

// CallPullSchemaAPIWithOptions fetches live schema with optional namespace and catalog controls.
func CallPullSchemaAPIWithOptions(endpoint, database, dbType, environment string, opts PullSchemaOptions) (*apitypes.PullSchemaResponse, error) {
	req := apitypes.PullSchemaRequest{
		Database:      database,
		Type:          dbType,
		Environment:   environment,
		Namespaces:    opts.Namespaces,
		CatalogDetail: opts.CatalogDetail,
		Lint:          opts.Lint,
	}
	var result apitypes.PullSchemaResponse
	if err := doPostInto(endpoint, "/api/pull", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallPlanAPI calls the plan API by reading .sql files from schemaDir.
// Files are grouped by namespace: subdirectories become namespace keys,
// flat files use the directory name as the namespace. Namespaces listed in
// ignoreNamespaces are excluded from the plan request. The second return
// value lists the namespaces actually removed by ignoreNamespaces so callers
// can disclose the exclusion alongside the plan.
//
// groupedExecution says whether the apply this plan is for hands the engine
// every ALTER at once or one table at a time. It only affects what the plan
// predicts about work already on the target; a caller that has not chosen yet
// passes false, the shape an apply runs without asking for anything else.
func CallPlanAPI(endpoint, database, dbType, environment, schemaDir, repo string, pr int, ignoreNamespaces []string, groupedExecution bool) (*apitypes.PlanResponse, []string, error) {
	schemaFiles, ignored, err := ReadSchemaFiles(schemaDir, environment, ignoreNamespaces)
	if err != nil {
		return nil, nil, fmt.Errorf("read schema files: %w", err)
	}
	if len(schemaFiles) == 0 {
		if len(ignored) > 0 {
			return nil, ignored, fmt.Errorf("no .sql files found in %s after excluding ignored namespaces %v", schemaDir, ignored)
		}
		return nil, nil, fmt.Errorf("no .sql files found in %s", schemaDir)
	}
	resp, err := postPlanRequest(endpoint, database, dbType, environment, schemaFiles, repo, pr, ignored, groupedExecution)
	if err != nil {
		return nil, ignored, err
	}
	return resp, ignored, nil
}

// CallPlanAPIWithFiles calls the plan API with pre-loaded, namespace-grouped schema files.
func CallPlanAPIWithFiles(endpoint, database, dbType, environment string, schemaFiles map[string]*apitypes.SchemaFiles, repo string, pr int) (*apitypes.PlanResponse, error) {
	return postPlanRequest(endpoint, database, dbType, environment, schemaFiles, repo, pr, nil, false)
}

// postPlanRequest posts a plan request. ignoredNamespaces names the
// namespaces removed from schemaFiles before the call — the server needs
// them to refuse engine shapes that cannot honor the exclusion.
func postPlanRequest(endpoint, database, dbType, environment string, schemaFiles map[string]*apitypes.SchemaFiles, repo string, pr int, ignoredNamespaces []string, groupedExecution bool) (*apitypes.PlanResponse, error) {
	req := apitypes.PlanRequest{
		Database:          database,
		Type:              dbType,
		Environment:       environment,
		SchemaFiles:       schemaFiles,
		Repository:        repo,
		IgnoredNamespaces: ignoredNamespaces,
		GroupedExecution:  groupedExecution,
	}
	if pr != 0 {
		prVal := int32(pr)
		req.PullRequest = &prVal
	}
	var result apitypes.PlanResponse
	if err := doPostInto(endpoint, "/api/plan", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallRollbackPlanAPI calls the rollback API to generate a plan that reverts
// the specified apply. The response includes database/environment metadata.
func CallRollbackPlanAPI(endpoint, applyID, environment string) (*apitypes.PlanResponse, error) {
	body := map[string]any{
		"apply_id":    applyID,
		"environment": environment,
	}
	var result apitypes.PlanResponse
	if err := doPostInto(endpoint, "/api/rollback/plan", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallApplyAPI calls the apply API and returns the typed result.
func CallApplyAPI(endpoint, planID, environment, caller string, options map[string]string) (*apitypes.ApplyResponse, error) {
	req := apitypes.ApplyRequest{
		PlanID:      planID,
		Environment: environment,
		Caller:      caller,
		Options:     options,
	}
	var result apitypes.ApplyResponse
	if err := doPostInto(endpoint, "/api/apply", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// callControlAPI posts an apply-scoped control request to path and decodes the
// typed response. Control operations share the same ControlRequest body and
// differ only by endpoint path and response type.
func callControlAPI[Resp any](endpoint, path, environment, applyID string) (*Resp, error) {
	req := apitypes.ControlRequest{Environment: environment, ApplyID: applyID, Caller: GenerateCLIOwner()}
	var result Resp
	if err := doPostInto(endpoint, path, req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallCutoverAPI calls the cutover API and returns the typed result.
func CallCutoverAPI(endpoint, environment, applyID string) (*apitypes.ControlResponse, error) {
	return callControlAPI[apitypes.ControlResponse](endpoint, "/api/cutover", environment, applyID)
}

// GetProgress fetches progress for a schema change by apply ID.
func GetProgress(endpoint, applyID string) (*apitypes.ProgressResponse, error) {
	return GetProgressCtx(context.Background(), endpoint, applyID)
}

// GetProgressCtx is like GetProgress but accepts a context for timeout/cancellation control.
func GetProgressCtx(ctx context.Context, endpoint, applyID string) (*apitypes.ProgressResponse, error) {
	var result apitypes.ProgressResponse
	if err := doGetIntoCtx(ctx, endpoint, fmt.Sprintf("/api/progress/apply/%s", applyID), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetDatabaseHistory fetches the apply history for a database.
func GetDatabaseHistory(endpoint, database, environment string) (*apitypes.DatabaseHistoryResponse, error) {
	path := fmt.Sprintf("/api/history/%s", database)
	if environment != "" {
		path += "?environment=" + environment
	}
	var result apitypes.DatabaseHistoryResponse
	if err := doGetInto(endpoint, path, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CheckActiveSchemaChange checks status for an active schema change on the
// database/environment pair. Returns nil if no active schema change is listed.
type ActiveSchemaChange struct {
	State   string
	ApplyID string
}

func CheckActiveSchemaChange(endpoint, database, environment string) (*ActiveSchemaChange, error) {
	var result apitypes.StatusResponse
	query := url.Values{}
	query.Set("environment", environment)
	query.Set("limit", "1000")
	// Ask only for applies still holding a target. This runs on every apply and
	// rollback preflight, and the answer never depends on settled history, so
	// scanning it would make every schema change pay for the environment's whole
	// past to learn whether one database is busy.
	query.Set("active", "true")
	if err := doGetInto(endpoint, "/api/status?"+query.Encode(), &result); err != nil {
		return nil, err
	}

	// The server stores and returns canonical keys, and the operator's flags
	// arrive in whatever case they were typed, so fold both sides before
	// comparing or a busy database slips past the preflight on a case mismatch.
	database = storage.CanonicalKey(database)
	environment = storage.CanonicalKey(environment)
	for _, apply := range result.Applies {
		if storage.CanonicalKey(apply.Database) != database || storage.CanonicalKey(apply.Environment) != environment {
			continue
		}
		// The server already excluded terminal states; re-checking here keeps the
		// answer correct if this ever reads a response that was not filtered.
		if state.IsTerminalApplyState(apply.State) {
			continue
		}
		return &ActiveSchemaChange{State: apply.State, ApplyID: apply.ApplyID}, nil
	}
	return nil, nil
}

// ReadSchemaFiles reads .sql files from a directory and groups them by namespace.
// Subdirectories become namespace keys; flat files use the directory name as the
// namespace (the MySQL database name). Only one level of subdirectories is
// supported (matching the webhook path behavior).
//
// The environment parameter enables $ENV substitution in namespace names.
// If non-empty, any "$ENV" in directory names or the default namespace is
// replaced with the environment value (e.g., "bikeshare_$ENV" → "bikeshare_staging").
//
// Namespaces listed in ignoreNamespaces (schemabot.yaml ignore_namespaces) are
// excluded from the result. The second return value lists the namespace keys
// actually removed by ignoreNamespaces, sorted.
func ReadSchemaFiles(dir string, environment string, ignoreNamespaces []string) (map[string]*apitypes.SchemaFiles, []string, error) {
	// Collect all files as relativePath → content
	rawFiles := make(map[string]string)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}

	for _, entry := range entries {
		// Follow symlinks: DirEntry.IsDir() returns false for symlinks even
		// if they point to directories. Use os.Stat to resolve.
		isDir := entry.IsDir()
		if !isDir {
			if info, err := os.Stat(filepath.Join(dir, entry.Name())); err == nil {
				isDir = info.IsDir()
			}
		}
		if isDir {
			// Read schema files inside the subdirectory
			subEntries, err := os.ReadDir(filepath.Join(dir, entry.Name()))
			if err != nil {
				return nil, nil, fmt.Errorf("read subdirectory %s: %w", entry.Name(), err)
			}
			for _, sub := range subEntries {
				if sub.IsDir() {
					continue
				}
				if !isSchemaFile(sub.Name()) {
					continue
				}
				// Use path.Join (forward slashes) for map keys so
				// GroupFilesByNamespace can parse them consistently.
				relPath := path.Join(entry.Name(), sub.Name())
				content, err := os.ReadFile(filepath.Join(dir, entry.Name(), sub.Name()))
				if err != nil {
					return nil, nil, fmt.Errorf("read %s: %w", relPath, err)
				}
				rawFiles[relPath] = string(content)
			}
			continue
		}
		if !isSchemaFile(entry.Name()) {
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, nil, fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		rawFiles[entry.Name()] = string(content)
	}

	// Group by namespace using the shared helper.
	// For flat files, the directory name is the database name.
	grouped, ignored, err := schema.GroupFilesByNamespace(rawFiles, filepath.Base(dir), environment, ignoreNamespaces)
	if err != nil {
		return nil, nil, err
	}

	// Convert schema.SchemaFiles → apitypes.SchemaFiles
	result := make(map[string]*apitypes.SchemaFiles, len(grouped))
	for ns, nsFiles := range grouped {
		result[ns] = &apitypes.SchemaFiles{Files: nsFiles.Files}
	}
	return result, ignored, nil
}

func isSchemaFile(name string) bool {
	return strings.HasSuffix(name, ".sql") || name == "vschema.json"
}

// CallStopAPI calls the stop API and returns the typed result.
func CallStopAPI(endpoint, environment, applyID string) (*apitypes.StopResponse, error) {
	return callControlAPI[apitypes.StopResponse](endpoint, "/api/stop", environment, applyID)
}

// CallCancelAPI calls the cancel API and returns the typed result.
func CallCancelAPI(endpoint, environment, applyID string) (*apitypes.CancelResponse, error) {
	return callControlAPI[apitypes.CancelResponse](endpoint, "/api/cancel", environment, applyID)
}

// CallStartAPI calls the start API and returns the typed result.
func CallStartAPI(endpoint, environment, applyID string) (*apitypes.StartResponse, error) {
	return callControlAPI[apitypes.StartResponse](endpoint, "/api/start", environment, applyID)
}

// CallReleaseAPI calls the release API and returns the typed result.
func CallReleaseAPI(endpoint, environment, applyID string) (*apitypes.ReleaseResponse, error) {
	return callControlAPI[apitypes.ReleaseResponse](endpoint, "/api/release", environment, applyID)
}

// CallRevertAPI calls the revert API and returns the typed result.
func CallRevertAPI(endpoint, environment, applyID string) (*apitypes.ControlResponse, error) {
	return callControlAPI[apitypes.ControlResponse](endpoint, "/api/revert", environment, applyID)
}

// CallSkipRevertAPI calls the skip-revert API and returns the typed result.
func CallSkipRevertAPI(endpoint, environment, applyID string) (*apitypes.ControlResponse, error) {
	return callControlAPI[apitypes.ControlResponse](endpoint, "/api/skip-revert", environment, applyID)
}

// ExitWithJSON outputs a JSON error response and exits with code 1.
// Returns nil so callers can use: return ExitWithJSON(...)
func ExitWithJSON(code, message string) error {
	resp := map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
	os.Exit(1)
	return nil // unreachable, but satisfies return type
}

// LockInfo represents lock information returned from the API.
type LockInfo struct {
	Database     string    `json:"database"`
	DatabaseType string    `json:"database_type"`
	Owner        string    `json:"owner"`
	Repository   string    `json:"repository"`
	PullRequest  int       `json:"pull_request"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// GenerateCLIOwner generates an owner identifier for CLI-based locks and the
// caller attribution on CLI-driven requests: "cli:<user>@<host>".
func GenerateCLIOwner() string {
	username := "unknown"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}

	hostname := "unknown"
	if h, err := os.Hostname(); err == nil {
		hostname = h
	}

	return cliOwner(username, hostname)
}

// cliOwner renders a CLI owner in the canonical spelling the lock API stores.
// The lock API folds every caller-supplied owner before persisting or matching
// it, so the CLI must fold the same value: an operator whose hostname or
// username carries uppercase characters would otherwise compare unequal to
// their own stored lock, and the pre-apply lock check would report the
// database as locked by someone else.
//
// The same string is also the caller attribution on CLI-driven requests, which
// the server does not fold. The hostname half is free — hostnames are
// case-insensitive — and an authenticated server records the verified subject
// in place of the claimed username, so nothing of the identity is lost there.
// A server running without API auth records the claimed username lowercased:
// a cosmetic loss on a display-only field, in exchange for an ownership
// predicate that agrees with the one the lock is stored under.
//
// The fold belongs here rather than in caller.FormatCLI because the server
// renders verified subjects through that same formatter, and a verified
// identity must reach storage in the spelling its provider issued.
func cliOwner(username, hostname string) string {
	return storage.CanonicalKey(caller.FormatCLI(username, hostname))
}

// AcquireLock attempts to acquire a lock on a database.
// Returns the lock info on success, or an error with ErrLockHeld if already locked.
func AcquireLock(endpoint, database, dbType, owner, repo string, pr int) (*LockInfo, error) {
	reqBody := map[string]any{
		"database":      database,
		"database_type": dbType,
		"owner":         owner,
	}
	if repo != "" {
		reqBody["repository"] = repo
	}
	if pr != 0 {
		reqBody["pull_request"] = pr
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint+"/api/locks/acquire", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, FormatConnectionError(endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	// Check for lock conflict (HTTP 409)
	if resp.StatusCode == http.StatusConflict {
		var result struct {
			Error       string    `json:"error"`
			CurrentLock *LockInfo `json:"current_lock"`
		}
		if err := json.Unmarshal(respBody, &result); err == nil && result.CurrentLock != nil {
			return result.CurrentLock, ErrLockHeld
		}
		return nil, ErrLockHeld
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("acquire lock failed: %s", FormatAPIError(resp.StatusCode, respBody))
	}

	var result struct {
		Lock *LockInfo `json:"lock"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return result.Lock, nil
}

// ReleaseLock releases a lock. Returns ErrLockNotOwned if not the owner.
func ReleaseLock(endpoint, database, dbType, owner string) error {
	err := doSendBody(endpoint, http.MethodDelete, "/api/locks", map[string]any{
		"database":      database,
		"database_type": dbType,
		"owner":         owner,
	})
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			// A 403 can also be an authorization denial naming the groups
			// that would grant access, so only the ownership error code maps
			// to ErrLockNotOwned; other denials surface the server's message.
			if apiErr.Status == http.StatusForbidden && apiErr.ErrorCode == apitypes.ErrCodeLockNotOwned {
				return ErrLockNotOwned
			}
			if apiErr.Status == http.StatusNotFound {
				return ErrLockNotFound
			}
		}
	}
	return err
}

// ForceReleaseLock releases a lock regardless of owner (admin override).
func ForceReleaseLock(endpoint, database, dbType string) error {
	err := doSendBody(endpoint, http.MethodDelete, "/api/locks", map[string]any{
		"database":      database,
		"database_type": dbType,
		"force":         true,
	})
	if IsNotFound(err) {
		return ErrLockNotFound
	}
	return err
}

// GetLock retrieves lock information for a database.
// Returns nil if no lock exists.
func GetLock(endpoint, database, dbType string) (*LockInfo, error) {
	var result struct {
		Lock *LockInfo `json:"lock"`
	}
	err := doGetInto(endpoint, fmt.Sprintf("/api/locks/%s/%s", database, dbType), &result)
	if IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result.Lock, nil
}

// ListLocks retrieves all active locks.
func ListLocks(endpoint string) ([]*LockInfo, error) {
	var result struct {
		Locks []*LockInfo `json:"locks"`
	}
	if err := doGetInto(endpoint, "/api/locks", &result); err != nil {
		return nil, err
	}
	return result.Locks, nil
}

// Lock error sentinels
var (
	ErrLockHeld     = fmt.Errorf("lock is already held by another owner")
	ErrLockNotOwned = fmt.Errorf("lock is not owned by you")
	ErrLockNotFound = fmt.Errorf("lock not found")
)

// StatusOptions controls the status list request.
type StatusOptions struct {
	Limit       int
	Environment string
	Deployment  string
	// State restricts the list to one apply state; empty means all states.
	State  string
	Failed bool
	// Last bounds the list to applies whose latest activity — on the apply or any
	// of its operations — falls within this window; zero means unbounded.
	Last time.Duration
	// Active restricts the list to applies that have not reached a terminal
	// state, so a caller asking whether a target is busy does not page through
	// settled history.
	Active bool
}

// GetStatus fetches recent schema changes.
func GetStatus(endpoint string, opts ...StatusOptions) (*apitypes.StatusResponse, error) {
	var result apitypes.StatusResponse
	requestPath := "/api/status"
	if len(opts) > 0 {
		values := url.Values{}
		if opts[0].Limit > 0 {
			values.Set("limit", strconv.Itoa(opts[0].Limit))
		}
		if opts[0].Environment != "" {
			values.Set("environment", opts[0].Environment)
		}
		if opts[0].Deployment != "" {
			values.Set("deployment", opts[0].Deployment)
		}
		if opts[0].State != "" {
			values.Set("state", opts[0].State)
		}
		if opts[0].Failed {
			values.Set("failed", "true")
		}
		if opts[0].Last > 0 {
			values.Set("last", opts[0].Last.String())
		}
		if opts[0].Active {
			values.Set("active", "true")
		}
		if encoded := values.Encode(); encoded != "" {
			requestPath += "?" + encoded
		}
	}
	if err := doGetInto(endpoint, requestPath, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// PlansOptions filters the stored-plan listing.
type PlansOptions struct {
	Limit       int
	Database    string
	Environment string
	// Repository restricts the listing to plans generated for PRs in that
	// repository (owner/name).
	Repository string
	// PullRequest restricts the listing to plans generated for that PR
	// number; the server requires Repository alongside it.
	PullRequest int
	// Last bounds the listing to plans created within this window; zero means
	// unbounded.
	Last time.Duration
}

// GetPlans retrieves recent stored plans as summaries, newest first.
func GetPlans(endpoint string, opts PlansOptions) (*apitypes.PlansResponse, error) {
	var result apitypes.PlansResponse
	values := url.Values{}
	if opts.Limit > 0 {
		values.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Database != "" {
		values.Set("database", opts.Database)
	}
	if opts.Environment != "" {
		values.Set("environment", opts.Environment)
	}
	if opts.Repository != "" {
		values.Set("repository", opts.Repository)
	}
	if opts.PullRequest > 0 {
		values.Set("pull_request", strconv.Itoa(opts.PullRequest))
	}
	if opts.Last > 0 {
		values.Set("last", opts.Last.String())
	}
	requestPath := "/api/plans"
	if encoded := values.Encode(); encoded != "" {
		requestPath += "?" + encoded
	}
	if err := doGetInto(endpoint, requestPath, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetStoredPlan retrieves one stored plan with its full content.
func GetStoredPlan(endpoint, planID string) (*apitypes.StoredPlanResponse, error) {
	var result apitypes.StoredPlanResponse
	if err := doGetInto(endpoint, "/api/plans/"+url.PathEscape(planID), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

type LogEntry = apitypes.LogEntry

// GetLogs retrieves apply logs for a database.
// If applyID is provided, it fetches logs for that specific apply.
// Otherwise, it fetches logs for the most recent apply in the environment.
// The response carries the newest limit entries plus whether older ones exist
// beyond the window.
func GetLogs(endpoint, database, environment, applyID string, limit int) (*apitypes.LogsResponse, error) {
	if database == "" && applyID == "" {
		return nil, fmt.Errorf("database or apply_id is required")
	}

	values := url.Values{}
	if applyID != "" {
		values.Set("apply_id", applyID)
	} else if environment != "" {
		values.Set("environment", environment)
	}
	if limit > 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	var path string
	if database == "" && applyID != "" {
		path = "/api/logs"
	} else {
		path = "/api/logs/" + url.PathEscape(database)
	}
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var result apitypes.LogsResponse
	if err := doGetInto(endpoint, path, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func GetDeploymentLogs(endpoint, applyID, deployment string, limit int) (*apitypes.DeploymentLogsResponse, error) {
	values := url.Values{}
	values.Set("apply_id", applyID)
	values.Set("deployment", deployment)
	if limit > 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	var result apitypes.DeploymentLogsResponse
	if err := doGetInto(endpoint, "/api/logs?"+values.Encode(), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Setting represents a key-value setting.
type Setting struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ListSettings retrieves all settings.
func ListSettings(endpoint string) ([]*Setting, error) {
	var result struct {
		Settings []*Setting `json:"settings"`
	}
	if err := doGetInto(endpoint, "/api/settings", &result); err != nil {
		return nil, err
	}
	return result.Settings, nil
}

// GetSetting retrieves the value of a specific setting.
func GetSetting(endpoint, key string) (string, error) {
	var result struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	err := doGetInto(endpoint, fmt.Sprintf("/api/settings/%s", key), &result)
	if IsNotFound(err) {
		return "", nil // Setting not found, return empty
	}
	if err != nil {
		return "", err
	}
	return result.Value, nil
}

// SetSetting sets the value of a setting.
func SetSetting(endpoint, key, value string) error {
	return doSendBody(endpoint, http.MethodPost, "/api/settings", map[string]string{
		"key":   key,
		"value": value,
	})
}
