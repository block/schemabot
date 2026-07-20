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

// checksRefreshedAttribution identifies what produced a checks-refreshed
// comment: the user who ran the command, or the system trigger when no user
// requested it (exactly one of the two is set).
type checksRefreshedAttribution struct {
	requestedBy string
	trigger     string
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
		if err := h.convergeAggregatesForNoManagedSchemaChanges(ctx, client, repo, pr, installationID, files, checksRefreshedAttribution{requestedBy: requestedBy}); err != nil {
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
// outcome, attributed to the user who ran the command or to the system
// trigger. A closed PR is rejected with an explicit error instead: its
// close-time cleanup owns the stored check state.
func (h *Handler) convergeAggregatesForNoManagedSchemaChanges(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, files []ghclient.PRFile, attribution checksRefreshedAttribution) error {
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
			h.logger.Info("aggregate participant staying silent on check refresh for closed PR with no managed schema changes",
				"repo", repo, "pr", pr, "requested_by", attribution.requestedBy, "trigger", attribution.trigger)
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
		h.logger.Info("aggregate participant staying silent on check refresh for PR with no managed schema changes",
			"repo", repo, "pr", pr, "head_sha", headSHA, "requested_by", attribution.requestedBy, "trigger", attribution.trigger)
		return nil
	}

	if h.leaderExpectsParticipantsForPR(repo, files) {
		h.logger.Info("check refresh found no leader-managed schema changes but expected participant paths are touched; aggregate gate will block until participants report",
			"repo", repo, "pr", pr, "head_sha", headSHA, "requested_by", attribution.requestedBy, "trigger", attribution.trigger)
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
		h.postComment(repo, pr, installationID, templates.RenderNoManagedSchemaChangesChecksRefreshed(templates.NoManagedSchemaChangesChecksRefreshedData{
			RequestedBy:    attribution.requestedBy,
			Trigger:        attribution.trigger,
			Timestamp:      timestamp,
			HeadSHA:        headSHA,
			GatedOnTenants: true,
		}))
		return nil
	}

	h.logger.Info("no managed schema changes; refreshing passing aggregate checks",
		"repo", repo, "pr", pr, "head_sha", headSHA, "requested_by", attribution.requestedBy, "trigger", attribution.trigger)
	h.postPassingAggregates(ctx, client, repo, pr, headSHA)
	h.postComment(repo, pr, installationID, templates.RenderNoManagedSchemaChangesChecksRefreshed(templates.NoManagedSchemaChangesChecksRefreshedData{
		RequestedBy: attribution.requestedBy,
		Trigger:     attribution.trigger,
		Timestamp:   timestamp,
		HeadSHA:     headSHA,
	}))
	return nil
}

// notifyRetainedStartedApplyBlocks posts the reconciliation-required comment
// for checks that stale cleanup just transitioned into the removed-schema
// block. The stored blocking reason is the durable dedupe signal: cleanup on
// later pushes finds the reason already set and does not reach this again, so
// the notice posts once per retained apply even across pods and webhook
// redeliveries.
func (h *Handler) notifyRetainedStartedApplyBlocks(ctx context.Context, repo string, pr int, installationID int64, checks []*storage.Check) {
	records := make([]schemaChangeReconciliationRecord, 0, len(checks))
	for _, check := range checks {
		record := schemaChangeReconciliationRecord{check: check}
		if check.ApplyID != 0 {
			apply, err := h.service.Storage().Applies().Get(ctx, check.ApplyID)
			switch {
			case err != nil:
				h.logger.Warn("reconciliation notice will omit apply details; failed to load apply for blocked check",
					"repo", repo, "pr", pr, "database", check.DatabaseName,
					"environment", check.Environment, "check_id", check.ID, "error", err)
			case apply == nil:
				h.logger.Warn("reconciliation notice will omit apply details; blocked check references a missing apply",
					"repo", repo, "pr", pr, "database", check.DatabaseName,
					"environment", check.Environment, "check_id", check.ID)
			default:
				record.apply = apply
			}
		}
		records = append(records, record)
	}

	h.logger.Info("posting reconciliation notice for retained started-apply blocks",
		"repo", repo, "pr", pr, "check_count", len(records))
	h.postComment(repo, pr, installationID, templates.RenderSchemaChangeReconciliationRequired(templates.SchemaChangeReconciliationData{
		Tenant:    h.deploymentTenant(),
		Trigger:   "Triggered automatically by a pull request update",
		Timestamp: templates.NowFunc().UTC().Format("2006-01-02 15:04:05"),
		Items:     schemaChangeReconciliationItems(records),
	}))
}

// convergeChecksAfterCompletedRollback refreshes a PR's check state after a
// rollback completes. The rollback restored the target schema, so a PR whose
// head no longer contains SchemaBot inputs matches the live database again —
// without this refresh the operator must know to comment `schemabot plan` to
// clear the rollback-completed block. A PR that still carries schema inputs
// keeps the block: its changes were rolled back and must be re-applied or
// re-planned deliberately. Callers invoke this only after winning the
// terminal check-state write, so the refresh runs at most once per rollback
// across pods and observer paths.
func (h *Handler) convergeChecksAfterCompletedRollback(ctx context.Context, client *ghclient.InstallationClient, a *storage.Apply) {
	prInfo, err := client.FetchPullRequest(ctx, a.Repository, a.PullRequest)
	if err != nil {
		h.logger.Warn("checks not refreshed after completed rollback; failed to fetch PR info",
			append(a.LogAttrs(), "error", err)...)
		return
	}
	// Rollback is a supported recovery path on closed PRs; close-time cleanup
	// owns their stored check state, so there is nothing to converge.
	if prInfo.IsClosed() {
		h.logger.Info("skipping check refresh after completed rollback: PR is closed", a.LogAttrs()...)
		return
	}
	files, err := client.FetchPRFiles(ctx, a.Repository, a.PullRequest)
	if err != nil {
		h.logger.Warn("checks not refreshed after completed rollback; failed to fetch PR files",
			append(a.LogAttrs(), "error", err)...)
		return
	}
	// The PR still declares schema changes: the rolled-back changes must be
	// re-applied or re-planned deliberately, so the rollback block stands.
	if prHasCurrentSchemaBotFiles(files) {
		h.logger.Info("skipping check refresh after completed rollback: PR still contains SchemaBot schema files",
			a.LogAttrs()...)
		return
	}
	records, err := h.schemaChangeReconciliationRecords(ctx, a.Repository, a.PullRequest, "", "")
	if err != nil {
		h.logger.Warn("checks not refreshed after completed rollback; failed to load apply-owned check state",
			append(a.LogAttrs(), "error", err)...)
		return
	}
	// Another apply still owns check state on this PR; its own terminal
	// refresh converges the aggregate when it finishes.
	if len(records) > 0 {
		h.logger.Info("skipping check refresh after completed rollback: other applies still own check state on this PR",
			append(a.LogAttrs(), "record_count", len(records))...)
		return
	}
	if err := h.convergeAggregatesForNoManagedSchemaChanges(ctx, client, a.Repository, a.PullRequest, a.InstallationID, files,
		checksRefreshedAttribution{trigger: "Triggered automatically after the rollback completed"}); err != nil {
		h.logger.Warn("failed to refresh checks after completed rollback", append(a.LogAttrs(), "error", err)...)
	}
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
				"check_status", check.Status)
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
