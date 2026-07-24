package webhook

import (
	"context"
	"fmt"
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

// assertBaseSchemaStillCurrent enforces the invariant that a PR must include
// every base-branch change under its managed schema directory before it can
// apply — a stale branch's plan would otherwise revert schema that already
// landed. It compares Git tree object IDs at the PR merge base and the
// current tip of the base branch (resolved from prInfo.BaseRef — the PR
// object's base SHA is a creation-time snapshot that never observes later
// base commits), so unrelated commits elsewhere in a monorepo never block an
// apply. GitHub read uncertainty rejects the apply rather than silently
// bypassing the guard.
//
// Returns follow the apply-gate disposition contract (see applyCommandCore):
//   - (true, err) — the base schema freshness could not be verified (a GitHub
//     read failed). It fails closed and posts a rejection comment, but the
//     returned error marks the rejection as an evaluation failure a durable
//     driver should re-drive.
//   - (true, nil) — rejected on merits (the schema path changed on the base
//     branch after the PR diverged). Terminal.
//   - (false, nil) — the base schema is unchanged under the PR; apply may proceed.
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
) (rejected bool, err error) {
	schemaPaths := []string{schema.SchemaPath}
	if schema.SchemaLinkPath != "" {
		schemaPaths = append(schemaPaths, schema.SchemaLinkPath)
	}
	changed, baseTipSHA, err := client.SchemaPathsChangedSinceMergeBase(ctx, repo, prInfo.BaseRef, prInfo.HeadSHA, schemaPaths)
	if err != nil {
		h.logger.Error("apply rejected: could not verify base schema freshness",
			"repo", repo,
			"pr", pr,
			"environment", environment,
			"database", schema.Database,
			"database_type", schema.Type,
			"schema_path", schema.SchemaPath,
			"base_ref", prInfo.BaseRef,
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
		return true, fmt.Errorf("verify base schema freshness %s#%d: %w", repo, pr, err)
	}
	if !changed {
		return false, nil
	}

	h.logger.Warn("apply rejected: schema path changed on base branch after PR divergence",
		"repo", repo,
		"pr", pr,
		"environment", environment,
		"database", schema.Database,
		"database_type", schema.Type,
		"schema_path", schema.SchemaPath,
		"base_ref", prInfo.BaseRef,
		"base_tip_sha", baseTipSHA,
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
	return true, nil
}
