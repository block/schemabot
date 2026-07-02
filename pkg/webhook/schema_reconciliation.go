package webhook

import (
	"context"
	"fmt"
	"path"
	"strings"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
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

	records, err := h.schemaChangeReconciliationRecords(ctx, repo, pr, environment, databaseName)
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

	h.logger.Debug("command found no apply-owned reconciliation state; continuing with normal config discovery",
		"repo", repo, "pr", pr, "environment", environment,
		"database", databaseName, "action", commandName)
	return false, nil
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
