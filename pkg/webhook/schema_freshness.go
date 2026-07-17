package webhook

import (
	"context"
	"strings"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// assertSchemaStillCurrent enforces the invariant that the schema files loaded
// at discovery still match the current PR HEAD on GitHub before any mutating
// (apply) or comment-rendering (plan) work runs.
//
// Discovery (CreateSchemaRequestFromPR) reads the PR HEAD once via the
// request-scoped cached FetchPullRequest and loads schema files at that SHA.
// Callers must pass freshHeadSHA from a separate FetchPullRequestNoCache call
// taken close to the point of use — that fresh SHA is the only TOCTOU-safe
// reference for "what is on the branch right now".
//
// If the two SHAs disagree, the discovery snapshot is stale: any plan rendered
// or apply executed against schema.SchemaFiles would be derived from a commit
// the branch is no longer on. The helper logs with operator-triage fields,
// increments schemabot.schema_freshness.rejected.total, posts a rejection
// comment, and returns true so the caller can release any locks and stop.
//
// Returns true to mean "rejected — caller must stop". Returns false when the
// snapshot is still current and execution may proceed.
func (h *Handler) assertSchemaStillCurrent(
	ctx context.Context,
	repo string,
	pr int,
	installationID int64,
	schema *ghclient.SchemaRequestResult,
	freshHeadSHA string,
	environment string,
	requestedBy string,
	action string,
) bool {
	if schema.HeadSHA == freshHeadSHA {
		return false
	}

	h.logger.Warn("rejected: schema discovery stale, PR HEAD advanced",
		"repo", repo,
		"pr", pr,
		"environment", environment,
		"database", schema.Database,
		"database_type", schema.Type,
		"discovery_sha", schema.HeadSHA,
		"current_sha", freshHeadSHA,
		"action", action,
		"requested_by", requestedBy,
	)

	metrics.RecordSchemaFreshnessRejected(ctx, metricActionKey(action), environment)

	h.postComment(repo, pr, installationID, templates.RenderStaleSchemaRejection(templates.StaleSchemaRejectionData{
		RequestedBy:  requestedBy,
		Database:     schema.Database,
		Environment:  environment,
		DiscoverySHA: schema.HeadSHA,
		CurrentSHA:   freshHeadSHA,
		Action:       action,
	}))

	return true
}

// metricActionKey converts a command-line action ("apply-confirm") to the
// underscore form expected by the metric's cardinality allowlist ("apply_confirm").
func metricActionKey(action string) string {
	return strings.ReplaceAll(action, "-", "_")
}

// assertBaseSchemaStillCurrent enforces the opt-in repository policy that a PR
// must include every base-branch change under its managed schema directory.
// It compares Git tree object IDs at the PR merge base and current base, so
// unrelated commits elsewhere in a monorepo never block an apply. GitHub read
// uncertainty rejects the apply rather than silently bypassing the guard.
func (h *Handler) assertBaseSchemaStillCurrent(
	ctx context.Context,
	client *ghclient.InstallationClient,
	repo string,
	pr int,
	installationID int64,
	schema *ghclient.SchemaRequestResult,
	prInfo *ghclient.PullRequestInfo,
	environment string,
	requestedBy string,
	action string,
) bool {
	config, ok := h.serverConfig()
	if !ok || !config.RequiresUpToDateWithBase(repo) {
		return false
	}

	schemaPaths := []string{schema.SchemaPath}
	if schema.SchemaLinkPath != "" {
		schemaPaths = append(schemaPaths, schema.SchemaLinkPath)
	}
	changed, err := client.SchemaPathsChangedSinceMergeBase(ctx, repo, prInfo.BaseSHA, prInfo.HeadSHA, schemaPaths)
	if err != nil {
		h.logger.Error("apply rejected: could not verify base schema freshness",
			"repo", repo,
			"pr", pr,
			"environment", environment,
			"database", schema.Database,
			"database_type", schema.Type,
			"schema_path", schema.SchemaPath,
			"base_sha", prInfo.BaseSHA,
			"head_sha", prInfo.HeadSHA,
			"action", action,
			"requested_by", requestedBy,
			"error", err,
		)
		metrics.RecordBaseSchemaFreshnessRejected(ctx, metricActionKey(action), environment, "verification_failed")
		h.postComment(repo, pr, installationID, templates.RenderBaseSchemaFreshnessRejection(templates.BaseSchemaFreshnessRejectionData{
			RequestedBy:       requestedBy,
			Database:          schema.Database,
			Environment:       environment,
			SchemaPath:        schema.SchemaPath,
			VerificationError: true,
		}))
		return true
	}
	if !changed {
		return false
	}

	h.logger.Warn("apply rejected: schema path changed on base branch after PR divergence",
		"repo", repo,
		"pr", pr,
		"environment", environment,
		"database", schema.Database,
		"database_type", schema.Type,
		"schema_path", schema.SchemaPath,
		"base_sha", prInfo.BaseSHA,
		"head_sha", prInfo.HeadSHA,
		"action", action,
		"requested_by", requestedBy,
	)
	metrics.RecordBaseSchemaFreshnessRejected(ctx, metricActionKey(action), environment, "stale")
	h.postComment(repo, pr, installationID, templates.RenderBaseSchemaFreshnessRejection(templates.BaseSchemaFreshnessRejectionData{
		RequestedBy: requestedBy,
		Database:    schema.Database,
		Environment: environment,
		SchemaPath:  schema.SchemaPath,
	}))
	return true
}
