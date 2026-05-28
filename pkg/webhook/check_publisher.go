package webhook

import (
	"context"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// verifyHeadSHAStillCurrentForPR returns false when writing status check state
// for headSHA would be unsafe because the PR now points at a different commit
// SHA. It records a metric and logs the reason before every false return so
// callers can stop without adding duplicate log noise.
func (h *Handler) verifyHeadSHAStillCurrentForPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, operation string) bool {
	if headSHA == "" {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  operation,
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("refusing to update status check without head SHA", "repo", repo, "pr", pr, "operation", operation)
		return false
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  operation,
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to verify status check head SHA before update",
			"repo", repo, "pr", pr, "head_sha", headSHA, "operation", operation, "error", err)
		return false
	}
	if prInfo.HeadSHA != headSHA {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  operation,
			Repository: repo,
			Status:     "stale",
		})
		h.logger.Info("skipping stale status check update because head SHA is no longer current for PR",
			"repo", repo, "pr", pr, "operation", operation,
			"stale_head_sha", headSHA, "current_head_sha", prInfo.HeadSHA)
		return false
	}
	return true
}

// updateAggregateCheck recomputes and creates/updates aggregate check runs that roll
// up per-database checks for a PR.
//
// When allowed_environments is configured, per-environment aggregates are created
// (e.g., "SchemaBot (staging)") that only roll up checks for that environment. This
// allows separate SchemaBot instances to each publish their own aggregate without
// conflicting with each other.
//
// When allowed_environments is NOT configured, a single "SchemaBot" aggregate is
// created that rolls up all per-database checks.
//
// Aggregate logic (first match wins):
//   - ANY check "in_progress"     → aggregate status "in_progress"
//   - ANY check "failure"         → aggregate "failure"
//   - ANY check "action_required" → aggregate "action_required"
//   - ALL checks "success"        → aggregate "success"
//   - NO per-database checks      → no aggregate (PR doesn't touch schema)
func (h *Handler) updateAggregateCheck(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string) {
	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "aggregate_check_sync") {
		return
	}

	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to fetch checks for aggregate", "repo", repo, "pr", pr, "error", err)
		return
	}

	// Filter out aggregate checks — only per-database checks contribute
	var dbChecks []*storage.Check
	for _, c := range checks {
		if !isAggregateCheck(c) {
			dbChecks = append(dbChecks, c)
		}
	}

	// No per-database checks means the PR doesn't touch schema files (or all check
	// records were already deleted by PR close cleanup). No aggregate to create.
	if len(dbChecks) == 0 {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "noop",
		})
		h.logger.Debug("no per-database checks for aggregate", "repo", repo, "pr", pr)
		return
	}

	config := h.service.Config()

	if len(config.AllowedEnvironments) > 0 {
		// Per-environment aggregates: create one aggregate per allowed environment.
		// Each uses the real environment name in the storage key to avoid collisions
		// between environments (e.g., staging vs production aggregates).
		for _, env := range config.AllowedEnvironments {
			envChecks := filterChecksByEnvironment(dbChecks, env)
			if len(envChecks) == 0 {
				continue
			}
			checkName := aggregateCheckNameForEnv(env)
			h.upsertAggregateCheckRun(ctx, client, repo, pr, headSHA, envChecks, checkName, env)
		}
	} else {
		// Single aggregate. Uses aggregateSentinel for the environment field
		// since there is no per-environment scoping.
		h.upsertAggregateCheckRun(ctx, client, repo, pr, headSHA, dbChecks, aggregateCheckName, aggregateSentinel)
	}
}

// upsertAggregateCheckRun computes the aggregate conclusion from the given checks
// and creates or updates a GitHub check run with the specified name.
//
// The environment parameter controls the storage key: for per-environment aggregates
// it is the real environment name (e.g., "staging"), for the global aggregate it is
// aggregateSentinel. DatabaseType and DatabaseName always use aggregateSentinel.
func (h *Handler) upsertAggregateCheckRun(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int, headSHA string,
	dbChecks []*storage.Check, checkName string, environment string,
) {
	conclusion, status := computeAggregate(dbChecks)
	title, summary := aggregateSummary(dbChecks, conclusion)

	opts := ghclient.CheckRunOptions{
		Name:   checkName,
		Status: status,
		Output: &ghclient.CheckRunOutput{
			Title:   title,
			Summary: summary,
		},
	}
	// GitHub requires conclusion only when status is "completed"
	if status == checkStatusCompleted {
		opts.Conclusion = conclusion
	}

	// Look up existing aggregate check state using the environment-specific key.
	existing, err := h.service.Storage().Checks().Get(ctx, repo, pr, environment, aggregateSentinel, aggregateSentinel)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: environment,
			Status:      "error",
		})
		h.logger.Error("failed to look up aggregate check", "repo", repo, "pr", pr, "environment", environment, "error", err)
		return
	}

	// Create a new check run if no existing record, or if the HEAD SHA changed
	// (new commit pushed). Updating an old check run tied to a previous SHA is
	// invisible on the PR — GitHub only shows checks for the HEAD commit.
	var checkRunID int64
	if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
		if err := client.UpdateCheckRun(ctx, repo, existing.CheckRunID, opts); err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: environment,
				Status:      "error",
			})
			h.logger.Error("failed to update aggregate check run",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", environment, "check_run_id", existing.CheckRunID,
				"head_sha", headSHA, "status", status,
				"conclusion", conclusion, "error", err)
			return
		}
		checkRunID = existing.CheckRunID
	} else {
		if existing != nil && existing.HeadSHA != headSHA {
			h.logger.Info("re-creating aggregate check on new HEAD SHA",
				"repo", repo, "pr", pr,
				"old_sha", existing.HeadSHA, "new_sha", headSHA)
		}
		id, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: environment,
				Status:      "error",
			})
			h.logger.Error("failed to create aggregate check run",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", environment, "head_sha", headSHA,
				"status", status, "conclusion", conclusion, "error", err)
			return
		}
		checkRunID = id
	}

	aggCheck := &storage.Check{
		Repository:   repo,
		PullRequest:  pr,
		HeadSHA:      headSHA,
		Environment:  environment,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   checkRunID,
		HasChanges:   conclusion != checkConclusionSuccess,
		Status:       status,
		Conclusion:   conclusion,
	}
	if err := h.service.Storage().Checks().Upsert(ctx, aggCheck); err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: environment,
			Status:      "error",
		})
		h.logger.Error("failed to store aggregate check state",
			"repo", repo, "pr", pr, "check_name", checkName,
			"environment", environment, "check_run_id", checkRunID,
			"head_sha", headSHA, "status", status,
			"conclusion", conclusion, "error", err)
		return
	}

	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:   "aggregate_check_sync",
		Repository:  repo,
		Environment: environment,
		Status:      "success",
	})
	h.logger.Info("aggregate check updated",
		"repo", repo, "pr", pr, "check_name", checkName,
		"environment", environment, "check_run_id", checkRunID,
		"status", status, "conclusion", conclusion,
		"per_database_checks", len(dbChecks))
}

// postPassingAggregates posts a passing aggregate check for each allowed environment.
// Called when this instance has no work to do for a PR — either because the PR doesn't
// touch schema files, or because the databases don't have environments this instance
// manages. Without this, branch protection would block indefinitely waiting for a
// check that would never come. It does not publish success over existing
// per-database state that still needs operator attention.
func (h *Handler) postPassingAggregates(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, title, summary string) {
	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "aggregate_check_sync") {
		return
	}

	config := h.service.Config()
	storedChecks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "error",
		})
		h.logger.Error("failed to fetch checks before passing aggregate", "repo", repo, "pr", pr, "error", err)
		return
	}

	type envCheck struct {
		name        string
		environment string
	}

	var checks []envCheck
	if len(config.AllowedEnvironments) > 0 {
		for _, env := range config.AllowedEnvironments {
			checks = append(checks, envCheck{
				name:        aggregateCheckNameForEnv(env),
				environment: env,
			})
		}
	} else {
		checks = append(checks, envCheck{
			name:        aggregateCheckName,
			environment: aggregateSentinel,
		})
	}

	h.logger.Debug("posting passing aggregates", "repo", repo, "pr", pr, "head_sha", headSHA, "count", len(checks))

	for _, ec := range checks {
		checkName := ec.name

		if hasBlockingCheckForEnvironment(storedChecks, ec.environment) {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "blocked",
			})
			h.logger.Info("skipping passing aggregate because stored checks still block",
				"repo", repo, "pr", pr, "check_name", checkName, "environment", ec.environment)
			continue
		}

		opts := ghclient.CheckRunOptions{
			Name:       checkName,
			Status:     checkStatusCompleted,
			Conclusion: checkConclusionSuccess,
			Output: &ghclient.CheckRunOutput{
				Title:   title,
				Summary: summary,
			},
		}

		existing, err := h.service.Storage().Checks().Get(ctx, repo, pr, ec.environment, aggregateSentinel, aggregateSentinel)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to look up aggregate check", "repo", repo, "pr", pr, "env", ec.environment, "error", err)
			continue
		}

		// Skip if already passing for this SHA
		if existing != nil && existing.HeadSHA == headSHA && existing.Conclusion == checkConclusionSuccess {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "noop",
			})
			h.logger.Debug("passing aggregate already exists", "repo", repo, "pr", pr, "check_name", checkName)
			continue
		}

		var checkRunID int64
		if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
			if err := client.UpdateCheckRun(ctx, repo, existing.CheckRunID, opts); err != nil {
				metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
					Operation:   "aggregate_check_sync",
					Repository:  repo,
					Environment: ec.environment,
					Status:      "error",
				})
				h.logger.Error("failed to update passing aggregate",
					"repo", repo, "pr", pr, "check_name", checkName,
					"environment", ec.environment, "check_run_id", existing.CheckRunID,
					"head_sha", headSHA, "error", err)
				continue
			}
			checkRunID = existing.CheckRunID
		} else {
			id, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
			if err != nil {
				metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
					Operation:   "aggregate_check_sync",
					Repository:  repo,
					Environment: ec.environment,
					Status:      "error",
				})
				h.logger.Error("failed to create passing aggregate",
					"repo", repo, "pr", pr, "check_name", checkName,
					"environment", ec.environment, "head_sha", headSHA, "error", err)
				continue
			}
			checkRunID = id
		}

		aggCheck := &storage.Check{
			Repository:   repo,
			PullRequest:  pr,
			HeadSHA:      headSHA,
			Environment:  ec.environment,
			DatabaseType: aggregateSentinel,
			DatabaseName: aggregateSentinel,
			CheckRunID:   checkRunID,
			HasChanges:   false,
			Status:       checkStatusCompleted,
			Conclusion:   checkConclusionSuccess,
		}
		if err := h.service.Storage().Checks().Upsert(ctx, aggCheck); err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to store passing aggregate check",
				"repo", repo, "pr", pr, "check_name", checkName,
				"environment", ec.environment, "check_run_id", checkRunID,
				"head_sha", headSHA, "error", err)
			continue
		}

		action := "created"
		if existing != nil && existing.CheckRunID != 0 && existing.HeadSHA == headSHA {
			action = "updated"
		}
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: ec.environment,
			Status:      "success",
		})
		h.logger.Info("posted passing aggregate",
			"repo", repo, "pr", pr, "check_name", checkName, "env", ec.environment, "action", action)
	}
}

// postFailingAggregates posts a failing aggregate check for each allowed environment
// that has errors. Called when all environments fail during planning so branch
// protection shows a clear failure instead of waiting indefinitely.
func (h *Handler) postFailingAggregates(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, errors map[string]string) {
	h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA, errors, checkBlockReason{})
}

// postFailingAggregatesWithBlock stores a blocking reason only for callers with
// a stable failure class. Generic plan errors should use postFailingAggregates.
func (h *Handler) postFailingAggregatesWithBlock(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA string, errors map[string]string, block checkBlockReason) {
	if !h.verifyHeadSHAStillCurrentForPR(ctx, client, repo, pr, headSHA, "aggregate_check_sync") {
		return
	}

	config := h.service.Config()

	type envCheck struct {
		name        string
		environment string
	}

	var checks []envCheck
	if len(config.AllowedEnvironments) > 0 {
		for _, env := range config.AllowedEnvironments {
			if _, hasError := errors[env]; hasError {
				checks = append(checks, envCheck{
					name:        aggregateCheckNameForEnv(env),
					environment: env,
				})
			}
		}
	} else {
		checks = append(checks, envCheck{
			name:        aggregateCheckName,
			environment: aggregateSentinel,
		})
	}

	if len(checks) == 0 {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_check_sync",
			Repository: repo,
			Status:     "noop",
		})
		h.logger.Debug("no failing aggregate checks to post", "repo", repo, "pr", pr)
		return
	}

	for _, ec := range checks {
		// Build summary from the error for this environment
		summary := "Plan failed"
		if errMsg, ok := errors[ec.environment]; ok {
			summary = errMsg
		} else if len(errors) > 0 {
			// Single-instance mode: use first error
			for _, msg := range errors {
				summary = msg
				break
			}
		}

		opts := ghclient.CheckRunOptions{
			Name:       ec.name,
			Status:     checkStatusCompleted,
			Conclusion: checkConclusionFailure,
			Output: &ghclient.CheckRunOutput{
				Title:   "Plan failed",
				Summary: summary,
			},
		}

		id, err := client.CreateCheckRun(ctx, repo, headSHA, opts)
		if err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to create failing aggregate", "repo", repo, "pr", pr, "error", err)
			continue
		}

		aggCheck := &storage.Check{
			Repository:   repo,
			PullRequest:  pr,
			HeadSHA:      headSHA,
			Environment:  ec.environment,
			DatabaseType: aggregateSentinel,
			DatabaseName: aggregateSentinel,
			CheckRunID:   id,
			HasChanges:   false,
			Status:       checkStatusCompleted,
			Conclusion:   checkConclusionFailure,
		}
		if block.blockingReason != "" {
			aggCheck.BlockingReason = block.blockingReason
			aggCheck.ErrorMessage = summary
		}
		if err := h.service.Storage().Checks().Upsert(ctx, aggCheck); err != nil {
			metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
				Operation:   "aggregate_check_sync",
				Repository:  repo,
				Environment: ec.environment,
				Status:      "error",
			})
			h.logger.Error("failed to store failing aggregate check", "error", err)
			continue
		}

		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:   "aggregate_check_sync",
			Repository:  repo,
			Environment: ec.environment,
			Status:      "success",
		})
		h.logger.Info("posted failing aggregate",
			"repo", repo, "pr", pr, "check_name", ec.name, "env", ec.environment)
	}
}
