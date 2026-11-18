package webhook

import (
	"context"
	"fmt"
	"slices"
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
// apply.
//
// Returns rejected=true only for a verified stale branch. GitHub read
// uncertainty stops the apply rather than silently bypassing the guard, and is
// returned as an error, not a rejection: freshness could not be verified, so
// the outcome is not the command's answer and a durable driver may re-drive
// it.
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
	changed, baseTipSHA, err := client.SchemaPathsChangedSinceMergeBase(ctx, repo, prInfo.BaseRef, prInfo.HeadSHA, environmentSchemaPaths(schema))
	if err != nil && len(changed) == 0 {
		h.logger.Error("apply stopped: could not verify base schema freshness",
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
		return false, fmt.Errorf("base schema freshness gate compare %s against %s tip for %s#%d: %w", schema.SchemaPath, prInfo.BaseRef, repo, pr, err)
	}
	if len(changed) == 0 {
		return false, nil
	}
	if err != nil {
		// A path already proven changed makes the branch stale, so the stale
		// rejection is the answer even though a later path could not be read.
		h.logger.Warn("base schema freshness gate found a changed schema path before a later path failed to read; rejecting the apply as stale",
			"repo", repo, "pr", pr, "environment", environment,
			"database", schema.Database, "database_type", schema.Type,
			"changed_paths", changed, "action", action, "error", err)
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

// environmentSchemaPaths are the paths whose content decides an environment's
// schema: its schema root and, when one names it, the environment's symlink.
// Retargeting the symlink changes the schema as surely as editing a file.
func environmentSchemaPaths(schema *ghclient.SchemaRequestResult) []string {
	paths := []string{schema.SchemaPath}
	if schema.SchemaLinkPath != "" {
		paths = append(paths, schema.SchemaLinkPath)
	}
	return paths
}

// planBaseSchema reports, for a single-environment plan comment, whether the
// PR's base branch changed the environment's schema after the PR branched off
// it. See planBaseSchemas.
func (h *Handler) planBaseSchema(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, environment string) *templates.BaseSchemaData {
	return h.planBaseSchemas(ctx, client, repo, pr, map[string]*ghclient.SchemaRequestResult{environment: schema})[environment]
}

// planBaseSchemas reports, for each environment of a plan comment, whether the
// PR's base branch changed that environment's schema after the PR branched
// off it. The plan diffs the branch's schema, so a branch missing a newer base
// change gets a plan that could undo it; the comment says so, and the apply
// gate (assertBaseSchemaStillCurrent) refuses to run until the branch has the
// change. A check GitHub cannot answer is shown as unverified rather than left
// out, and the apply gate checks again and fails closed.
//
// Environments planned from the same head share one comparison: the base
// branch tip and merge base are resolved once and every environment's schema
// paths are compared in it, so a plan comment's GitHub cost does not grow with
// the number of environments beyond the tree levels their paths add (AV-15).
func (h *Handler) planBaseSchemas(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schemas map[string]*ghclient.SchemaRequestResult) map[string]*templates.BaseSchemaData {
	results := make(map[string]*templates.BaseSchemaData, len(schemas))
	envsByHead := map[string][]string{}
	for env, schema := range schemas {
		envsByHead[schema.HeadSHA] = append(envsByHead[schema.HeadSHA], env)
	}
	unverified := func(baseBranch string, envs []string) {
		for _, env := range envs {
			results[env] = &templates.BaseSchemaData{BaseBranch: baseBranch, Unverified: true}
		}
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		for headSHA, envs := range envsByHead {
			h.logger.Warn("plan comment will say the base branch could not be checked for newer schema changes: fetch PR failed; apply checks again before it runs",
				append(baseSchemaLogAttrs(repo, pr, headSHA, envs, schemas), "error", err)...)
			unverified("", envs)
		}
		return results
	}
	for headSHA, envs := range envsByHead {
		slices.Sort(envs)
		logAttrs := append(baseSchemaLogAttrs(repo, pr, headSHA, envs, schemas), "base_ref", prInfo.BaseRef)
		var paths []string
		for _, env := range envs {
			for _, path := range environmentSchemaPaths(schemas[env]) {
				if !slices.Contains(paths, path) {
					paths = append(paths, path)
				}
			}
		}
		changed, baseTipSHA, err := client.SchemaPathsChangedSinceMergeBase(ctx, repo, prInfo.BaseRef, headSHA, paths)
		if err != nil && len(changed) == 0 {
			h.logger.Warn("plan comment will say the base branch could not be checked for newer schema changes; apply checks again before it runs",
				append(logAttrs, "error", err)...)
			unverified(prInfo.BaseRef, envs)
			continue
		}
		for _, env := range envs {
			var envChanged []string
			for _, path := range environmentSchemaPaths(schemas[env]) {
				if slices.Contains(changed, path) {
					envChanged = append(envChanged, path)
				}
			}
			if err != nil && len(envChanged) == 0 {
				// A later path failed to read before this environment's paths
				// were all checked, so nothing proves it current.
				h.logger.Warn("plan comment will say the base branch could not be checked for newer schema changes: a schema path failed to read; apply checks again before it runs",
					append(logAttrs, "environment", env, "error", err)...)
				unverified(prInfo.BaseRef, []string{env})
				continue
			}
			if len(envChanged) > 0 {
				attrs := append(slices.Clone(logAttrs), "environment", env, "base_tip_sha", baseTipSHA, "changed_paths", envChanged)
				if err != nil {
					// The paths named may not be all of them, since a later one
					// failed to read; the branch is stale either way.
					attrs = append(attrs, "unchecked_paths_error", err)
				}
				h.logger.Warn("plan is of a branch missing newer schema changes from its base branch; the plan comment says so and apply is blocked until the branch has them", attrs...)
			}
			results[env] = &templates.BaseSchemaData{BaseBranch: prInfo.BaseRef, ChangedPaths: envChanged}
		}
	}
	return results
}

// baseSchemaLogAttrs identifies the plan a base-schema check ran for.
func baseSchemaLogAttrs(repo string, pr int, headSHA string, envs []string, schemas map[string]*ghclient.SchemaRequestResult) []any {
	first := schemas[envs[0]]
	return []any{
		"repo", repo, "pr", pr, "environments", envs,
		"database", first.Database, "database_type", first.Type, "head_sha", headSHA,
	}
}
