package webhook

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// autoPlanSizeOf measures what an automatic plan of configs would cover: the
// databases it would plan and the changed schema files under their config
// directories. A file renamed out of a directory counts by the path it left.
// Both answers come from the PR's changed-file list and discovery, so measuring
// costs no GitHub reads beyond the ones discovery already made.
func autoPlanSizeOf(files []ghclient.PRFile, configs []ghclient.DiscoveredConfig) (api.AutoPlanSize, []string) {
	var databases []string
	for _, cfg := range configs {
		if !slices.Contains(databases, cfg.Config.Database) {
			databases = append(databases, cfg.Config.Database)
		}
	}
	slices.Sort(databases)

	schemaFiles := 0
	for _, file := range files {
		if underAnyConfigDir(file.Filename, configs) && ghclient.IsSchemaFile(file.Filename) {
			schemaFiles++
			continue
		}
		if file.PreviousFilename != "" && underAnyConfigDir(file.PreviousFilename, configs) && ghclient.IsSchemaFile(file.PreviousFilename) {
			schemaFiles++
		}
	}
	return api.AutoPlanSize{Databases: len(databases), SchemaFiles: schemaFiles}, databases
}

// underAnyConfigDir reports whether path sits in the directory of one of
// configs, at any depth. A config at the repository root covers every path.
func underAnyConfigDir(path string, configs []ghclient.DiscoveredConfig) bool {
	for _, cfg := range configs {
		if cfg.SchemaDir == "" || cfg.SchemaDir == "." || strings.HasPrefix(path, cfg.SchemaDir+"/") {
			return true
		}
	}
	return false
}

// refuseOversizedAutoPlan answers an automatic plan of a PR over the
// deployment's auto_plan_limits. It plans nothing: every database and every
// environment would read each schema file the PR changes, out of the App
// installation's shared GitHub budget. Instead it fails the aggregate check
// closed with a stored block, so no later recompute can pass the PR until an
// automatic plan of a smaller head clears it (MG-1), and posts one comment
// naming the limit and the way forward. The caller has already verified that
// headSHA is still the PR's head.
//
// An error means the block could not be stored. The comment is not posted
// then, so a durable delivery retries the whole refusal.
func (h *Handler) refuseOversizedAutoPlan(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, headSHA, source, action, beforeSHA, deliveryID string, size api.AutoPlanSize, databases []string) error {
	limits := h.service.Config().AutoPlanLimits
	checksEnabled := h.shouldPublishChecks(ctx, repo, "auto_plan_size_limit")
	data := templates.AutoPlanSizeLimitData{
		Databases:        databases,
		SchemaFiles:      size.SchemaFiles,
		MaxDatabases:     limits.DatabaseLimit(),
		MaxSchemaFiles:   limits.SchemaFileLimit(),
		TooManyDatabases: limits.TooManyDatabases(size),
		GatesMerge:       checksEnabled,
		Tenant:           h.service.Config().Tenant,
	}
	h.logger.Warn("auto-plan refused because the PR is larger than one automatic plan covers; no schema is read",
		"repo", repo, "pr", pr, "head_sha", headSHA, "source", source, "delivery_id", deliveryID,
		"databases", size.Databases, "schema_files", size.SchemaFiles,
		"max_databases", data.MaxDatabases, "max_schema_files", data.MaxSchemaFiles,
		"checks_enabled", checksEnabled)
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "auto_plan_size_limit",
		Repository: repo,
		Status:     "blocked",
	})

	// Decide on the comment before the block below records this head, since
	// the decision reads which head the stored block was recorded for.
	alreadyShown, err := h.autoPlanSizeRefusalAlreadyShown(ctx, client, repo, pr, action, beforeSHA, headSHA)
	if err != nil {
		h.logger.Warn("auto-plan size limit comment will be posted because the earlier refusal could not be confirmed current",
			"repo", repo, "pr", pr, "head_sha", headSHA, "before_sha", beforeSHA, "error", err)
		alreadyShown = false
	}

	if checksEnabled {
		block := autoPlanSizeLimitBlock
		block.message = autoPlanSizeLimitCheckSummary(data)
		if !h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA,
			h.aggregateMessagesForAllEnvironments(block.message), block) {
			return fmt.Errorf("store the auto-plan size limit block for %s#%d at %s", repo, pr, headSHA)
		}
	} else {
		h.logger.Info("auto-plan size limit stores no block because status checks are disabled for this repository; nothing gates its merge",
			"repo", repo, "pr", pr, "head_sha", headSHA)
	}

	if alreadyShown {
		h.logger.Info("auto-plan size limit comment skipped because the previous head was refused the same way and this push changed no schema inputs",
			"repo", repo, "pr", pr, "head_sha", headSHA, "before_sha", beforeSHA, "source", source, "delivery_id", deliveryID)
		return nil
	}
	h.postComment(repo, pr, installationID, templates.RenderAutoPlanSizeLimit(data))
	return nil
}

// autoPlanSizeRefusalAlreadyShown reports whether a push left the PR's
// refusal comment current: the push's previous head holds the stored size
// block, so that head's refusal comment was posted, and the push changed no
// schema inputs, so the PR is refused for the same reason. Only a synchronize
// delivery with a previous head can tell.
func (h *Handler) autoPlanSizeRefusalAlreadyShown(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, action, beforeSHA, headSHA string) (bool, error) {
	if action != "synchronize" || beforeSHA == "" {
		return false, nil
	}
	refused, err := h.headHoldsAutoPlanSizeBlock(ctx, repo, pr, beforeSHA)
	if err != nil {
		return false, err
	}
	if !refused {
		h.logger.Info("auto-plan size limit comment will be posted because the previous head holds no stored size block",
			"repo", repo, "pr", pr, "head_sha", headSHA, "before_sha", beforeSHA)
		return false, nil
	}
	files, err := client.FetchChangedFilesBetween(ctx, repo, beforeSHA, headSHA)
	if err != nil {
		return false, fmt.Errorf("compare %s...%s in %s: %w", beforeSHA, headSHA, repo, err)
	}
	return !ghclient.HasSchemaInputFiles(files), nil
}

// headHoldsAutoPlanSizeBlock reports whether an aggregate check of the PR
// stores the size block for headSHA.
func (h *Handler) headHoldsAutoPlanSizeBlock(ctx context.Context, repo string, pr int, headSHA string) (bool, error) {
	for _, target := range h.aggregateCheckTargetsForRepo(repo) {
		stored, err := h.service.Storage().Checks().Get(ctx, repo, pr, target.environment, aggregateSentinel, aggregateSentinel)
		if err != nil {
			return false, fmt.Errorf("read aggregate check for %s#%d (env %s): %w", repo, pr, target.environment, err)
		}
		if stored != nil && stored.HeadSHA == headSHA && stored.BlockingReason == checkstate.BlockAutoPlanSizeLimit {
			return true, nil
		}
	}
	return false, nil
}

// autoPlanSizeLimitCheckSummary is the aggregate Check Run summary for a PR
// over the auto-plan limits. A Check Run summary renders no code spans, so it
// carries counts and the remedy, and leaves the database names and the command
// to the comment.
func autoPlanSizeLimitCheckSummary(data templates.AutoPlanSizeLimitData) string {
	return "This PR is too large to plan automatically. " + templates.AutoPlanSizeLimitReason(data) +
		" SchemaBot planned none of it. Split this PR into smaller PRs; the PR comment names a command that plans one database meanwhile."
}

// refuseApplyOnOversizedPR refuses an apply or an apply-confirm, named by
// command, on a PR whose aggregate check carries the auto-plan size block. An
// apply of one database would leave that database changed by a PR that cannot
// merge until it is split, so the target and the default branch would disagree
// for as long as the PR stays open. Explicit plans stay allowed, since they
// change nothing. A repository with status checks disabled stores no block,
// since nothing gates its merge, so its applies are not refused.
//
// It reports whether it refused. An error means the stored check state could
// not be read, which fails the command closed and is retryable.
func (h *Handler) refuseApplyOnOversizedPR(ctx context.Context, repo string, pr int, installationID int64, command, environment, requestedBy string, result CommandResult) (bool, error) {
	key := environment
	if len(h.service.Config().AllowedEnvironments) == 0 {
		key = aggregateSentinel
	}
	aggregate, err := h.service.Storage().Checks().Get(ctx, repo, pr, key, aggregateSentinel, aggregateSentinel)
	if err != nil {
		h.logger.Error("apply refused because the aggregate check state could not be read to rule out the auto-plan size limit",
			"repo", repo, "pr", pr, "command", command, "environment", environment, "error", err)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, command, environment, requestedBy, "Failed to read this PR's check state. Retry, and see server logs if it persists.")
		}
		return false, fmt.Errorf("read aggregate check for %s#%d (env %s): %w", repo, pr, environment, err)
	}
	if aggregate == nil || aggregate.BlockingReason != checkstate.BlockAutoPlanSizeLimit {
		return false, nil
	}
	h.logger.Info("apply refused because the PR is larger than one automatic plan covers",
		"repo", repo, "pr", pr, "command", command, "environment", environment, "head_sha", aggregate.HeadSHA,
		"blocking_reason", aggregate.BlockingReason)
	h.postCommandError(repo, pr, installationID, command, environment, requestedBy,
		"This PR is too large to plan automatically, so SchemaBot will not apply it. Split it into smaller PRs, then apply each one.")
	return true, nil
}
