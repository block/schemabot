package webhook

import (
	"context"
	"fmt"
	"path"
	"strings"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

type schemaChangeReconciliationRecord struct {
	check *storage.Check
	apply *storage.Apply
}

func (h *Handler) handleNoManagedSchemaChangesForCommand(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, commandName, environment, databaseName, requestedBy string) (bool, error) {
	files, err := client.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		return false, fmt.Errorf("fetch PR files before %s: %w", commandName, err)
	}
	if prHasCurrentSchemaBotFiles(files) {
		h.logger.Debug("command found current SchemaBot-related files in PR; continuing with normal config discovery",
			"repo", repo, "pr", pr, "environment", environment,
			"database", databaseName, "action", commandName)
		return false, nil
	}

	// A plan with no named database will converge the checks below, and that
	// refresh spans every environment's aggregate — so the apply-owned guard
	// must too: a plan scoped with -e must not converge checks while another
	// environment's apply still owns this PR's state.
	convergesChecks := commandName == action.Plan && databaseName == ""
	recordScopeEnv := environment
	if convergesChecks {
		recordScopeEnv = ""
	}

	records, err := h.schemaChangeReconciliationRecords(ctx, repo, pr, recordScopeEnv, databaseName)
	if err != nil {
		return true, err
	}
	if len(records) > 0 {
		h.logger.Info("command blocked because current PR no longer contains an apply-owned schema change",
			"repo", repo, "pr", pr, "environment", environment,
			"database", databaseName, "action", commandName,
			"record_count", len(records))
		h.postComment(repo, pr, installationID, templates.RenderSchemaChangeReconciliationRequired(templates.SchemaChangeReconciliationData{
			Tenant:      h.deploymentTenant(),
			RequestedBy: requestedBy,
			Timestamp:   templates.NowFunc().UTC().Format("2006-01-02 15:04:05"),
			Items:       schemaChangeReconciliationItems(records),
		}))
		return true, nil
	}

	// With no current SchemaBot inputs and no apply-owned state, a plan
	// command's remaining useful work is converging the checks: the PR may
	// predate check enablement for the repo or have lost the webhook delivery
	// that would have created its check, leaving branch protection waiting on
	// a check nothing else will create. Recreate the aggregate on the current
	// head the same way auto-plan does for a PR with no schema files. An
	// explicitly named database is an explicit ask for that database's plan
	// and proceeds through normal config discovery instead.
	if convergesChecks {
		if err := h.convergeAggregatesForNoManagedSchemaChanges(ctx, client, repo, pr, installationID, files, requestedBy); err != nil {
			return true, err
		}
		return true, nil
	}

	h.logger.Debug("command found no apply-owned reconciliation state; continuing with normal config discovery",
		"repo", repo, "pr", pr, "environment", environment,
		"database", databaseName, "action", commandName)
	return false, nil
}

// convergeAggregatesForNoManagedSchemaChanges recreates the aggregate check
// state for a PR with no managed schema changes, mirroring the auto-plan
// behavior for a PR with no schema files: an aggregate participant stays
// silent (the leader owns the required check), a leader whose expected
// participant paths are touched routes through the aggregate fold (which
// fails closed until every expected participant reports), and otherwise
// passing aggregates are posted on the current head. A comment reports the
// outcome to the user who ran the command. A closed PR is rejected with an
// explicit error instead: its close-time cleanup owns the stored check state.
func (h *Handler) convergeAggregatesForNoManagedSchemaChanges(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, files []ghclient.PRFile, requestedBy string) error {
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return fmt.Errorf("fetch PR info to refresh checks for PR with no managed schema changes: %w", err)
	}
	// A closed PR has nothing to converge: close-time cleanup already settled
	// its stored check state, and recreating Check Runs here would resurrect
	// rows that cleanup is authoritative over. Participants stay silent as on
	// open PRs; otherwise the user gets an explicit error instead of a
	// "refreshed as passing" comment on a PR that can never merge.
	if prInfo.IsClosed() {
		if h.isAggregateParticipant(repo) {
			h.logger.Info("aggregate participant staying silent on plan for closed PR with no managed schema changes",
				"repo", repo, "pr", pr, "requested_by", requestedBy)
			return nil
		}
		return fmt.Errorf("PR #%d in %s is closed; SchemaBot refreshes check state only for open PRs", pr, repo)
	}

	headSHA := prInfo.HeadSHA
	timestamp := templates.NowFunc().UTC().Format("2006-01-02 15:04:05")

	// Stale plan-only checks from commits whose schema changes were since
	// reverted would keep the aggregate blocked, so clean them up first —
	// the same cleanup auto-plan runs before posting aggregates. Apply-owned
	// records were already handled by the caller; cleanup independently
	// retains them as blocking.
	h.cleanupStaleChecks(repo, pr, headSHA, installationID, nil)

	if h.isAggregateParticipant(repo) {
		h.logger.Info("aggregate participant staying silent on plan for PR with no managed schema changes",
			"repo", repo, "pr", pr, "head_sha", headSHA, "requested_by", requestedBy)
		return nil
	}

	if h.leaderExpectsParticipantsForPR(repo, files) {
		h.logger.Info("plan found no leader-managed schema changes but expected participant paths are touched; aggregate gate will block until participants report",
			"repo", repo, "pr", pr, "head_sha", headSHA, "requested_by", requestedBy)
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
		h.postComment(repo, pr, installationID, templates.RenderNoManagedSchemaChangesChecksRefreshed(templates.NoManagedSchemaChangesChecksRefreshedData{
			RequestedBy:    requestedBy,
			Timestamp:      timestamp,
			HeadSHA:        headSHA,
			GatedOnTenants: true,
		}))
		return nil
	}

	h.logger.Info("plan found no managed schema changes; refreshing passing aggregate checks",
		"repo", repo, "pr", pr, "head_sha", headSHA, "requested_by", requestedBy)
	h.postPassingAggregates(ctx, client, repo, pr, headSHA)
	h.postComment(repo, pr, installationID, templates.RenderNoManagedSchemaChangesChecksRefreshed(templates.NoManagedSchemaChangesChecksRefreshedData{
		RequestedBy: requestedBy,
		Timestamp:   timestamp,
		HeadSHA:     headSHA,
	}))
	return nil
}

func prHasCurrentSchemaBotFiles(files []ghclient.PRFile) bool {
	for _, file := range files {
		if strings.HasSuffix(file.Filename, ".sql") || strings.HasSuffix(file.Filename, "vschema.json") {
			return true
		}
		if path.Base(file.Filename) == ghclient.ConfigFileName && !strings.EqualFold(file.Status, "removed") {
			return true
		}
	}
	return false
}

func (h *Handler) schemaChangeReconciliationRecords(ctx context.Context, repo string, pr int, environment, databaseName string) ([]schemaChangeReconciliationRecord, error) {
	if h.service == nil || h.service.Storage() == nil {
		return nil, fmt.Errorf("SchemaBot storage is not configured for schema change reconciliation checks")
	}

	checks, err := h.service.Storage().Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("load stored check state for schema change reconciliation repo %s pr %d: %w", repo, pr, err)
	}

	var records []schemaChangeReconciliationRecord
	for _, check := range checks {
		if check == nil {
			h.logger.Warn("skipping nil stored check during schema change reconciliation",
				"repo", repo, "pr", pr, "environment", environment,
				"database", databaseName)
			continue
		}
		if isAggregateCheck(check) {
			h.logger.Debug("skipping aggregate stored check during schema change reconciliation",
				"repo", repo, "pr", pr, "environment", check.Environment,
				"check_id", check.ID)
			continue
		}
		if environment != "" && check.Environment != environment {
			h.logger.Debug("skipping stored check for a different environment during schema change reconciliation",
				"repo", repo, "pr", pr, "requested_environment", environment,
				"check_environment", check.Environment, "database", check.DatabaseName,
				"check_id", check.ID)
			continue
		}
		if databaseName != "" && check.DatabaseName != databaseName {
			h.logger.Debug("skipping stored check for a different database during schema change reconciliation",
				"repo", repo, "pr", pr, "requested_database", databaseName,
				"check_database", check.DatabaseName, "environment", check.Environment,
				"check_id", check.ID)
			continue
		}
		if !checkHasStartedApply(check) {
			h.logger.Debug("skipping plan-only stored check during schema change reconciliation",
				"repo", repo, "pr", pr, "database", check.DatabaseName,
				"environment", check.Environment, "check_id", check.ID)
			continue
		}

		var apply *storage.Apply
		if check.ApplyID != 0 {
			apply, err = h.service.Storage().Applies().Get(ctx, check.ApplyID)
			if err != nil {
				return nil, fmt.Errorf("load apply %d for schema change reconciliation repo %s pr %d database %s environment %s: %w",
					check.ApplyID, repo, pr, check.DatabaseName, check.Environment, err)
			}
			if apply == nil {
				h.logger.Warn("stored check references missing apply during schema change reconciliation; check will still block",
					"repo", repo, "pr", pr, "database", check.DatabaseName,
					"environment", check.Environment, "check_id", check.ID,
					"apply_id", check.ApplyID)
			}
		} else {
			h.logger.Warn("stored check has started-apply state without an apply ID during schema change reconciliation; check will still block",
				"repo", repo, "pr", pr, "database", check.DatabaseName,
				"environment", check.Environment, "check_id", check.ID,
				"status", check.Status)
		}

		records = append(records, schemaChangeReconciliationRecord{check: check, apply: apply})
	}

	return records, nil
}

func schemaChangeReconciliationItems(records []schemaChangeReconciliationRecord) []templates.SchemaChangeReconciliationItem {
	items := make([]templates.SchemaChangeReconciliationItem, 0, len(records))
	for _, record := range records {
		item := templates.SchemaChangeReconciliationItem{
			Database:    record.check.DatabaseName,
			Environment: record.check.Environment,
			InProgress:  record.check.Status == checkStatusInProgress,
		}
		if record.apply != nil {
			item.ApplyID = record.apply.ApplyIdentifier
			item.State = record.apply.State
			item.InProgress = !state.IsTerminalApplyState(record.apply.State)
		}
		items = append(items, item)
	}
	return items
}
