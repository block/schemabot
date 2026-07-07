package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// pullRequestPayload represents the relevant fields from a GitHub pull_request webhook.
type pullRequestPayload struct {
	Action      string `json:"action"`
	Before      string `json:"before"`
	PullRequest struct {
		Number int  `json:"number"`
		Merged bool `json:"merged"`
		Head   struct {
			SHA string `json:"sha"`
			Ref string `json:"ref"`
		} `json:"head"`
		User struct {
			Login string `json:"login"`
		} `json:"user"`
	} `json:"pull_request"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

// handlePullRequest processes GitHub pull_request webhook events.
// On PR open/synchronize/reopen, it auto-plans all databases with schema changes.
func (h *Handler) handlePullRequest(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte) {
	var payload pullRequestPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid pull_request payload")
		return
	}

	// Repo-level webhook deliveries carry no installation id in the payload; the
	// dispatcher resolves it and stashes it on the context.
	installationID := h.effectiveInstallationID(ctx, payload.Installation.ID)

	// Route PR actions
	switch payload.Action {
	case "opened", "synchronize", "reopened":
		// proceed to auto-plan below
	case "closed":
		h.goSafe(payload.Repository.FullName, payload.PullRequest.Number, installationID, func() {
			h.handlePRClosed(payload.Repository.FullName, payload.PullRequest.Number, installationID, payload.PullRequest.Merged)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "PR close cleanup started"})
		return
	default:
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "pull_request action ignored",
		})
		return
	}

	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	repo := payload.Repository.FullName
	pr := payload.PullRequest.Number
	headSHA := payload.PullRequest.Head.SHA

	// Reject webhooks from repositories not in the configured allowlist
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("webhook from unregistered repository",
			"event", "pull_request",
			"action", payload.Action,
			"repo", repo,
			"pr", pr,
			"installation_id", installationID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "pull_request", payload.Action, repo)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "repository not registered",
		})
		return
	}

	h.logger.Info("auto-plan triggered",
		"action", payload.Action,
		"repo", repo,
		"pr", pr,
		"head_sha", headSHA,
	)

	ctx, cancel, client, err := h.autoPlanBootstrap(repo, installationID)
	if err != nil {
		h.logger.Error("failed to bootstrap auto-plan", "repo", repo, "pr", pr, "head_sha", headSHA, "error", err)
		h.writeError(w, http.StatusInternalServerError, "failed to initialize GitHub client")
		return
	}
	defer cancel()

	message := h.runAutoPlanForPR(ctx, client, repo, pr, headSHA, installationID, "pull_request", payload.Action, payload.Before)
	h.writeJSON(w, http.StatusOK, map[string]string{"message": message})
}

func (h *Handler) autoPlanBootstrap(repo string, installationID int64) (context.Context, context.CancelFunc, *ghclient.InstallationClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// Dedupe FetchPullRequest calls within this webhook delivery.
	ctx = ghclient.WithPRInfoCache(ctx)

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		cancel()
		return nil, nil, nil, fmt.Errorf("create GitHub client for repo %s installation %d: %w", repo, installationID, err)
	}
	return ctx, cancel, client, nil
}

func (h *Handler) shouldPostAutoPlanComment(ctx context.Context, client *ghclient.InstallationClient, action, repo string, pr int, beforeSHA, headSHA string) bool {
	if action != "synchronize" {
		return true
	}
	if beforeSHA == "" {
		h.logger.Info("auto-plan will post plan comment because synchronize payload has no previous HEAD SHA",
			"repo", repo, "pr", pr, "head_sha", headSHA)
		return true
	}
	files, err := client.FetchChangedFilesBetween(ctx, repo, beforeSHA, headSHA)
	if err != nil {
		h.logger.Warn("auto-plan will post plan comment because changed files could not be compared",
			"repo", repo, "pr", pr, "before_sha", beforeSHA, "head_sha", headSHA, "error", err)
		return true
	}
	if ghclient.HasSchemaInputFiles(files) {
		return true
	}
	h.logger.Info("auto-plan will refresh checks without posting a plan comment because synchronize changed no schema inputs",
		"repo", repo, "pr", pr, "before_sha", beforeSHA, "head_sha", headSHA)
	return false
}

func (h *Handler) runAutoPlanForPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, installationID int64, source string, action string, beforeSHA string) string {
	// Fetch the changed files once so the same list drives both config discovery
	// and the server-managed-directory safety check below.
	files, err := client.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to fetch PR files for auto-plan", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "error", err)
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, headSHA, err)
		return "config discovery failed"
	}

	// Discover all configs matching changed schema files in this PR
	configs, err := client.FindConfigsForPRFiles(ctx, repo, headSHA, files)
	if err != nil {
		h.logger.Error("failed to discover configs for PR", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "error", err)
		h.postConfigDiscoveryFailure(ctx, client, repo, pr, headSHA, err)
		return "config discovery failed"
	}

	// Fail closed when the PR changes schema files under a directory the server
	// config manages but no schemabot.yaml resolves for them — e.g. the PR
	// removed the config while keeping schema changes. Dropping the config must
	// not silently unmanage a server-owned schema directory.
	if unmanaged := h.unmanagedServerManagedSchemaChanges(repo, files, configs); len(unmanaged) > 0 {
		h.failClosedOnUnmanagedSchemaDir(ctx, client, repo, pr, headSHA, source, unmanaged)
		return "schema change under managed directory has no config"
	}

	configs = h.filterManagedDiscoveredConfigs(ctx, repo, pr, headSHA, source, configs)

	// Config discovery and the managed-directory guard just re-verified this
	// commit, and the clear re-checks allowed-environment coverage for every
	// discovered database. Together those cover every condition that records
	// an aggregate blocking reason, so a stored block can now be released.
	h.clearAggregateBlocksForVerifiedPR(ctx, client, repo, pr, headSHA, configs)

	// Collect database names from discovered configs
	affectedDatabases := make(map[string]bool)
	for _, cfg := range configs {
		affectedDatabases[cfg.Config.Database] = true
	}

	// Clean up stale checks from databases no longer in the PR.
	// Pass the new HEAD SHA so cleanup can create new check runs on the correct commit.
	h.goSafe(repo, pr, installationID, func() {
		h.cleanupStaleChecks(repo, pr, headSHA, installationID, affectedDatabases)
	})

	if len(configs) == 0 {
		h.logger.Info("no schema files in PR, skipping auto-plan", "repo", repo, "pr", pr, "head_sha", headSHA, "source", source)
		// An aggregate participant does not own the required check for the repo —
		// the leader does — so on a PR that touches none of this deployment's
		// schema it has nothing to report and stays silent, rather than posting a
		// passing check that only adds a per-tenant row near the merge button. The
		// leader publishes the required check and gates on participants' own
		// checks when they exist, so a silent participant cannot wedge branch
		// protection (its check is non-required by the aggregate contract).
		if h.isAggregateParticipant(repo) {
			h.logger.Info("aggregate participant staying silent on PR with no managed schema changes",
				"repo", repo, "pr", pr, "head_sha", headSHA, "source", source)
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:  "aggregate_participant_skip",
				Repository: repo,
				Status:     "skipped",
			})
			return "no schema files in PR (aggregate participant, staying silent)"
		}
		// A PR with no leader-managed schema can still touch schema owned by
		// expected participant deployments. The leader's aggregate is the
		// required check, so it must gate on those participants' Check Runs —
		// route through the aggregate fold, which fails closed until every
		// expected participant reports terminal success, instead of posting an
		// unconditional passing aggregate. The fold re-runs as participants'
		// Check Run events arrive, converging to passing once they succeed.
		if h.leaderExpectsParticipantsForPR(repo, files) {
			h.logger.Info("no leader-managed schema in PR but expected participant paths are touched; aggregate gate will block until participants report",
				"repo", repo, "pr", pr, "head_sha", headSHA, "source", source)
			h.goSafe(repo, pr, installationID, func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				c, err := h.clientForRepo(repo, installationID)
				if err != nil {
					h.logger.Error("failed to create GitHub client for participant-gated aggregate",
						"repo", repo, "pr", pr, "head_sha", headSHA, "error", err)
					return
				}
				h.updateAggregateCheck(ctx, c, repo, pr, headSHA)
			})
			return "no schema files in PR (aggregate folds expected participants)"
		}
		// Post passing aggregates on the current HEAD SHA so branch protection
		// isn't blocked on PRs that don't touch schema files. Always post —
		// on synchronize events the HEAD SHA changes, so the aggregate must be
		// recreated on the new commit. If stale per-database check records exist,
		// cleanupStaleChecks (above) also updates the aggregate — both converge
		// to the same result (passing aggregate on new SHA) so the overlap is safe.
		h.goSafe(repo, pr, installationID, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := h.clientForRepo(repo, installationID)
			if err != nil {
				h.logger.Error("failed to create GitHub client for passing aggregate", "error", err)
				return
			}
			h.postPassingAggregates(ctx, c, repo, pr, headSHA)
		})
		return "no schema files in PR"
	}
	postPlanComment := h.shouldPostAutoPlanComment(ctx, client, action, repo, pr, beforeSHA, headSHA)

	// Launch auto-plan for each discovered config
	tenant := ""
	if config := h.service.Config(); config != nil {
		tenant = config.Tenant
	}
	for _, cfg := range configs {
		database := cfg.Config.Database
		h.goSafe(repo, pr, installationID, func() {
			h.handleMultiEnvPlan(repo, pr, database, tenant, installationID, "", true, postPlanComment, 0)
		})
	}

	return "auto-plan started"
}

func (h *Handler) filterManagedDiscoveredConfigs(ctx context.Context, repo string, pr int, headSHA, source string, configs []ghclient.DiscoveredConfig) []ghclient.DiscoveredConfig {
	managed := configs[:0]
	for _, cfg := range configs {
		if cfg.Config == nil {
			h.logger.Warn("discovered schema config is missing parsed config and will be ignored",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"schema_path", cfg.SchemaDir, "source", source)
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:  "schema_config_discovery",
				Repository: repo,
				Status:     "skipped",
			})
			continue
		}
		if h.shouldProcessSchemaConfig(ctx, repo, pr, headSHA, cfg.Config.Database, string(cfg.Config.GetType()), cfg.SchemaDir, source) {
			managed = append(managed, cfg)
		}
	}
	return managed
}

func (h *Handler) postConfigDiscoveryFailure(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, discoveryErr error) {
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "schema_config_discovery",
		Repository: repo,
		Status:     "error",
	})

	block := configDiscoveryFailedBlock
	if ghclient.IsUnavailableError(discoveryErr) {
		block = githubConfigDiscoveryUnavailableBlock
	}
	h.logger.Info("posting failing aggregate for config discovery failure",
		"repo", repo, "pr", pr, "head_sha", headSHA,
		"blocking_reason", block.blockingReason)
	h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA,
		h.aggregateMessagesForAllEnvironments(block.message), block)
}

func (h *Handler) aggregateMessagesForAllEnvironments(message string) map[string]string {
	allowed := h.service.Config().AllowedEnvironments
	if len(allowed) == 0 {
		return map[string]string{aggregateSentinel: message}
	}

	messages := make(map[string]string, len(allowed))
	for _, env := range allowed {
		messages[env] = message
	}
	return messages
}

// handlePRClosed cleans up resources when a PR is closed (merged or unmerged):
// it releases locks held by this PR and deletes its stored check state.
//
// Cleanup only covers finished work. While any apply for the PR is
// non-terminal, that apply's database lock is retained so another PR cannot
// acquire the database mid-apply, and the PR's stored check state is retained
// so a close-and-reopen cannot convert in-flight apply state into a passing
// check. Apply-owned check state that reached a terminal state without
// concluding successfully is also retained, so a close-and-reopen cannot
// bypass a block that requires operator reconciliation. On a close without
// merge, apply-owned check state that concluded successfully is retained too:
// the stored success may predate a commit that removed the applied change,
// and only reopen-time cleanup can re-verify it against the PR contents. If
// apply state cannot be read, cleanup fails closed and nothing is released
// or deleted.
func (h *Handler) handlePRClosed(repo string, pr int, _ int64, merged bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A closed PR gets no more scheduled re-folds; drop its budget entry so the
	// in-memory map does not accumulate one entry per closed PR. A reopen folds
	// fresh and re-arms with a full budget.
	h.clearParticipantRefoldBudget(repo, pr)

	applies, err := h.service.Storage().Applies().GetByPR(ctx, repo, pr)
	if err != nil {
		// Fail closed: with apply state unknown, releasing a lock or deleting
		// check state could unblock a database with an apply still in flight.
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "pr_close_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("PR close cleanup skipped: cannot verify apply state; all locks and check state are retained",
			"repo", repo, "pr", pr, "error", err)
		return
	}

	inFlight := h.inFlightAppliesForClosedPR(ctx, repo, pr, applies)

	h.releaseLocksForClosedPR(ctx, repo, pr, inFlight)

	if len(inFlight) > 0 {
		h.logger.Info("check state retained for closed PR until all applies reach a terminal state",
			"repo", repo, "pr", pr, "in_flight_databases", len(inFlight))
		return
	}

	// Delete stored check state for this PR. Apply-owned rows that still block
	// survive the delete at the storage layer: in-flight rows (even if the
	// applies table missed the in-flight work above) and terminal rows whose
	// conclusion is not success, such as a schema change removed from the PR
	// after its apply started. Those blocks require operator reconciliation
	// and must persist across a close and reopen. On an unmerged close,
	// apply-owned rows that concluded successfully survive too: the stored
	// success may predate a commit that removed the applied change, so
	// reopen-time cleanup must re-verify it before the row can stop blocking.
	if err := h.service.Storage().Checks().DeleteByPRRetainingBlockingApplyOwned(ctx, repo, pr, merged); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "pr_close_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to delete checks for closed PR", "repo", repo, "pr", pr, "merged", merged, "error", err)
	} else {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "pr_close_cleanup",
			Repository: repo,
			Status:     "success",
		})
		if merged {
			h.logger.Info("deleted checks for merged PR; apply-owned rows that still block are retained",
				"repo", repo, "pr", pr)
		} else {
			h.logger.Info("deleted plan-only checks for unmerged closed PR; all apply-owned rows are retained",
				"repo", repo, "pr", pr)
		}
	}
}

// closedPRDatabase identifies the database lock an apply holds.
type closedPRDatabase struct {
	database     string
	databaseType string
}

// inFlightAppliesForClosedPR returns the databases for which the closed PR
// still has a non-terminal apply recorded. Each in-flight apply is logged and
// counted because it blocks close cleanup: the database stays locked and the
// PR's stored check state stays in place until the apply reaches a terminal
// state.
func (h *Handler) inFlightAppliesForClosedPR(ctx context.Context, repo string, pr int, applies []*storage.Apply) map[closedPRDatabase]bool {
	inFlight := make(map[closedPRDatabase]bool)
	for _, a := range applies {
		if state.IsTerminalApplyState(a.State) {
			// Terminal applies never block close cleanup.
			continue
		}
		inFlight[closedPRDatabase{database: a.Database, databaseType: a.DatabaseType}] = true
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "pr_close_cleanup",
			Repository:   repo,
			Database:     a.Database,
			DatabaseType: a.DatabaseType,
			Environment:  a.Environment,
			Status:       "blocked",
		})
		h.logger.Warn("retaining lock and check state for closed PR with in-flight apply; close cleanup skipped for this database",
			"repo", repo, "pr", pr,
			"database", a.Database, "database_type", a.DatabaseType,
			"environment", a.Environment,
			"apply_id", a.ID, "apply_identifier", a.ApplyIdentifier, "apply_state", a.State)
	}
	return inFlight
}

// releaseLocksForClosedPR releases the closed PR's locks, except locks on
// databases that still have an in-flight apply.
func (h *Handler) releaseLocksForClosedPR(ctx context.Context, repo string, pr int, inFlight map[closedPRDatabase]bool) {
	locks, err := h.service.Storage().Locks().GetByPR(ctx, repo, pr)
	if err != nil {
		h.logger.Error("failed to look up locks for closed PR; no locks released", "repo", repo, "pr", pr, "error", err)
		return
	}
	for _, lock := range locks {
		if inFlight[closedPRDatabase{database: lock.DatabaseName, databaseType: lock.DatabaseType}] {
			h.logger.Info("lock retained on PR close because an apply is in flight",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType)
			continue
		}
		if err := h.service.Storage().Locks().Release(ctx, lock.DatabaseName, lock.DatabaseType, lock.Owner); err != nil {
			h.logger.Error("failed to release lock on PR close",
				"repo", repo, "pr", pr, "database", lock.DatabaseName, "database_type", lock.DatabaseType, "error", err)
		} else {
			h.logger.Info("released lock on PR close",
				"repo", repo, "pr", pr, "database", lock.DatabaseName)
		}
	}
}

// cleanupStaleChecks updates checks for databases no longer in the PR.
// Plan-only checks can be marked "success" because the current PR no longer asks
// SchemaBot to apply anything. Checks that represent a started apply remain
// blocking because the live database may already have changed or may still change.
//
// On synchronize events, headSHA is the new commit SHA. Stale checks must be created
// as new check runs on this SHA (not updated on the old SHA) so GitHub shows them
// on the current commit.
func (h *Handler) cleanupStaleChecks(repo string, pr int, headSHA string, installationID int64, affectedDatabases map[string]bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if h.service == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("cannot clean up stale checks without service", "repo", repo, "pr", pr, "head_sha", headSHA)
		return
	}
	if h.service.Storage() == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("cannot clean up stale checks without storage", "repo", repo, "pr", pr, "head_sha", headSHA)
		return
	}

	client, clientErr := h.clientForRepo(repo, installationID)
	if clientErr != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to create GitHub client for stale cleanup", "repo", repo, "pr", pr, "head_sha", headSHA, "error", clientErr)
		return
	}
	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "stale_check_cleanup") {
		return
	}

	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to get checks for stale cleanup", "repo", repo, "pr", pr, "error", err)
		return
	}

	cleaned := false

	for _, check := range checks {
		if isAggregateCheck(check) {
			h.logger.Debug("skipping aggregate check during stale cleanup",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}

		if affectedDatabases[check.DatabaseName] {
			h.logger.Debug("skipping check during stale cleanup because database is still affected",
				"repo", repo, "pr", pr, "head_sha", headSHA,
				"database", check.DatabaseName, "database_type", check.DatabaseType,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}

		// This check's database is no longer in the PR.
		h.logger.Info("cleaning up stale check",
			"repo", repo, "pr", pr,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "head_sha", headSHA,
			"previous_status", check.Status, "previous_conclusion", check.Conclusion,
			"previous_blocking_reason", check.BlockingReason, "apply_id", check.ApplyID)

		if checkHasStartedApply(check) {
			if h.blockStaleStartedApplyCheckState(ctx, repo, pr, headSHA, check) {
				cleaned = true
			}
			continue
		}

		if h.markStalePlanOnlyCheckStateSuccessful(ctx, repo, pr, headSHA, check) {
			cleaned = true
		}
	}

	// Recompute aggregate on the new HEAD SHA after cleaning up stale checks
	if cleaned {
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	} else {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "stale_check_cleanup",
			Repository: repo,
			Status:     "noop",
		})
	}
}

func (h *Handler) blockStaleStartedApplyCheckState(ctx context.Context, repo string, pr int, headSHA string, check *storage.Check) bool {
	check.HeadSHA = headSHA
	check.HasChanges = true
	check.BlockingReason = schemaRemovedAfterApplyBlock.blockingReason
	check.ErrorMessage = schemaRemovedAfterApplyBlock.message
	if check.Status == checkStatusInProgress {
		check.Conclusion = ""
	} else {
		check.Status = checkStatusCompleted
		check.Conclusion = checkConclusionActionRequired
	}
	if err := h.service.Storage().Checks().Upsert(ctx, check); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "stale_check_cleanup",
			Repository:   repo,
			Database:     check.DatabaseName,
			DatabaseType: check.DatabaseType,
			Environment:  check.Environment,
			Status:       "error",
		})
		h.logger.Error("failed to block stale check with started apply",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"apply_id", check.ApplyID, "error", err)
		return false
	}
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "stale_check_cleanup",
		Repository:   repo,
		Database:     check.DatabaseName,
		DatabaseType: check.DatabaseType,
		Environment:  check.Environment,
		Status:       "blocked",
	})
	return true
}

func (h *Handler) markStalePlanOnlyCheckStateSuccessful(ctx context.Context, repo string, pr int, headSHA string, check *storage.Check) bool {
	priorApplyID := check.ApplyID
	check.HeadSHA = headSHA
	check.Conclusion = checkConclusionSuccess
	check.HasChanges = false
	check.Status = checkStatusCompleted
	check.ApplyID = 0
	check.BlockingReason = ""
	check.ErrorMessage = ""

	// The success write is guarded against in-flight apply-owned rows: an apply
	// that started after this cleanup read the row must keep blocking the PR.
	marked, err := h.service.Storage().Checks().MarkStalePlanSuccessful(ctx, check)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "stale_check_cleanup",
			Repository:   repo,
			Database:     check.DatabaseName,
			DatabaseType: check.DatabaseType,
			Environment:  check.Environment,
			Status:       "error",
		})
		h.logger.Error("failed to mark stale plan check successful",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"prior_apply_id", priorApplyID, "error", err)
		return false
	}

	if !marked {
		// A concurrent apply claimed the row between the cleanup read and this
		// write. Leave it in_progress and apply-owned so it keeps blocking until
		// an operator reconciles the target environment.
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "stale_check_cleanup",
			Repository:   repo,
			Database:     check.DatabaseName,
			DatabaseType: check.DatabaseType,
			Environment:  check.Environment,
			Status:       "blocked",
		})
		h.logger.Warn("stale plan check left blocking because an apply started concurrently; the check gate will block PR applies until an operator reconciles the target",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", check.DatabaseName, "database_type", check.DatabaseType,
			"environment", check.Environment, "check_id", check.ID,
			"prior_apply_id", priorApplyID)
		return true
	}

	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "stale_check_cleanup",
		Repository:   repo,
		Database:     check.DatabaseName,
		DatabaseType: check.DatabaseType,
		Environment:  check.Environment,
		Status:       "success",
	})
	return true
}
