package webhook

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// reviewTimeDrift computes the review-time drift rollup for a database and
// environment and turns it into the check-record outcome plus the preview
// rendering of that rollup.
//
// It returns a clean outcome when every deployment matches the reviewed plan,
// and a blocked outcome on any divergence, a deployment that could not be diffed
// or compared, or a failure to compute the rollup at all: without a trustworthy
// comparison SchemaBot cannot confirm the reviewed change is safe to apply
// everywhere, so the plan check fails closed.
//
// The primary plan reporting errors is not a drift signal — that generic plan
// failure already fails the check on its own — so the rollup is skipped and the
// outcome is not-evaluated, avoiding N-1 live per-deployment diffs whose result
// the rollup would classify entirely from the unusable baseline anyway.
//
// On a successful rollup the preview data is nil for a single-deployment
// database (nothing to compare) and non-nil otherwise, so the PR comment can
// show a uniform "same plan everywhere" line or a per-deployment breakdown even
// when the reviewed primary plan is a clean no-op. On a rollup failure the
// preview is always non-nil (Computed:false), regardless of deployment count,
// so the PR comment explains why the check is failing closed.
//
// primaryPlan is the reviewed primary plan proto returned by
// executePlanProtoWithTransientRetry, reused as the rollup baseline so the
// comparison is against exactly what was reviewed. primaryMember is the
// deployment and target that plan was created against.
func (h *Handler) reviewTimeDrift(ctx context.Context, planReq api.PlanRequest, primaryPlan *ternv1.PlanResponse, primaryMember routing.ExecutionTarget, repo string, pr int) (reviewDriftOutcome, *templates.DeploymentDriftData) {
	if len(primaryPlan.GetErrors()) > 0 {
		h.logger.Debug("skipping review-time drift rollup: primary plan reported errors",
			"repo", repo, "pr", pr, "database", planReq.Database, "environment", planReq.Environment)
		return reviewDriftOutcome{state: driftNotEvaluated}, nil
	}

	rollup, err := h.service.RollupReviewTimeDrift(ctx, planReq, primaryPlan, primaryMember)
	if err != nil {
		h.logger.Error("review-time drift rollup failed; the plan check will block the PR closed",
			"repo", repo,
			"pr", pr,
			"database", planReq.Database,
			"environment", planReq.Environment,
			"error", err)
		// Keep the stored Change column short and stable; the root cause is in the
		// log above, and the raw error is untrusted for the aggregate's markdown.
		// The preview says drift could not be verified so reviewers see why the
		// check is failing closed.
		return reviewDriftOutcome{
			state:   driftBlocked,
			summary: "drift check failed; see logs",
		}, &templates.DeploymentDriftData{Computed: false}
	}
	preview := deploymentDriftPreview(rollup, primaryPlan.GetPlanId())
	if rollup.Clean {
		return reviewDriftOutcome{state: driftClean}, preview
	}
	return reviewDriftOutcome{
		state:   driftBlocked,
		summary: summarizeReviewDrift(rollup),
	}, preview
}

// erroredDriftDetail is the sanitized detail shown in the PR preview for a
// deployment whose diff could not be computed. The raw error is untrusted for
// PR markdown (it can carry internal hostnames, IPs, or DSN fragments) and is
// logged server-side instead.
const erroredDriftDetail = "diff failed; see server logs"

// deploymentDriftPreview turns a computed rollup into the PR-preview rendering
// data. It returns nil for a single-deployment database: with one deployment
// there is nothing to compare, so a "same plan everywhere" line would be noise.
func deploymentDriftPreview(rollup api.PlanRollup, reviewedPlanID string) *templates.DeploymentDriftData {
	if len(rollup.Entries) <= 1 {
		return nil
	}
	entries := make([]templates.DeploymentDriftEntry, len(rollup.Entries))
	for i, e := range rollup.Entries {
		entry := templates.DeploymentDriftEntry{
			Deployment: e.Deployment,
			Target:     e.Target,
			Primary:    i == 0,
			Class:      e.Class.String(),
		}
		switch e.Class {
		case api.DeploymentDiverged:
			entry.Detail = describeDriftDiff(e.Diff)
		case api.DeploymentErrored:
			// The raw diff error can wrap dial failures carrying internal
			// hostnames, IPs, or DSN fragments, so it stays out of the PR
			// markdown. The root cause is logged with the deployment in
			// RollupReviewTimeDrift; the preview shows only a sanitized line.
			entry.Detail = erroredDriftDetail
		}
		entries[i] = entry
	}
	independent := rollup.Planning == api.PlanIndependent
	data := &templates.DeploymentDriftData{
		Deployments: entries,
		Clean:       rollup.Clean,
		Computed:    true,
		Independent: independent,
	}
	// Grouping describes the targets that were planned, so it is only meaningful
	// once every one of them was. A blocked rollup lists each member on its own
	// instead: the operator's next step is the member that could not be planned,
	// not the plans of an apply that cannot run.
	//
	// Mirrored members are left ungrouped because a clean mirrored rollup has
	// already proved they are one group. Saying so a second time, in the
	// vocabulary of a fleet that may diverge, would suggest the agreement was an
	// outcome rather than the requirement that let the check pass.
	if rollup.Clean && independent {
		data.Plans = deploymentPlanGroups(rollup, reviewedPlanID)
	}
	return data
}

// deploymentPlanGroups groups the rollout's members by the plan each would run,
// one entry per distinct plan.
//
// Members are grouped on the plan fingerprint, which two members share exactly
// when their plans are the same work — so a group can be described once and
// attributed to all of its members without comparing every pair. Groups come out
// in the rollout order of their first member, with the primary's group first:
// the reviewed plan is the one an operator has already seen, and a fixed order
// keeps a comment that is re-rendered on a later push from reshuffling under a
// reader who is looking for what changed.
func deploymentPlanGroups(rollup api.PlanRollup, reviewedPlanID string) []templates.DeploymentPlanGroup {
	names := rollupMemberNames(rollup)
	var groups []templates.DeploymentPlanGroup
	byPlan := make(map[string]int, len(rollup.Entries))
	for i, e := range rollup.Entries {
		at, ok := byPlan[e.PlanFingerprint]
		if !ok {
			// The primary runs the reviewed plan itself, so it has no member plan
			// row of its own and its identifier is the reviewed plan's.
			planID := e.PlanIdentifier
			if i == 0 {
				planID = reviewedPlanID
			}
			groups = append(groups, templates.DeploymentPlanGroup{
				Primary: i == 0,
				Changes: memberPlanChanges(e.ChangeSet),
				PlanID:  planID,
			})
			at = len(groups) - 1
			byPlan[e.PlanFingerprint] = at
		}
		groups[at].Members = append(groups[at].Members, names[i])
	}
	// The primary is the first member, so its group is already first. Ordering is
	// stated as a property of the result rather than left to that coincidence,
	// which a later change to rollout order would silently break.
	slices.SortStableFunc(groups, func(a, b templates.DeploymentPlanGroup) int {
		switch {
		case a.Primary == b.Primary:
			return 0
		case a.Primary:
			return -1
		default:
			return 1
		}
	})
	return groups
}

// memberPlanChanges renders one member's plan in the shape the comment renders
// the reviewed plan in, so a group's changes are described by the same code that
// describes the plan a reviewer has already read.
//
// A sharded namespace carries its changes twice: once per shard, and once in a
// collapsed namespace view that dedupes tables across shards. Both are kept, the
// same way the reviewed plan keeps them, so the rendering can show what applies
// where rather than a namespace-level view that hides a shard.
//
// A namespace that appears only on shard rows still gets an entry. Dropping it
// would silently remove work from a plan the comment claims to describe in full.
func memberPlanChanges(cs tern.ChangeSet) []templates.KeyspaceChangeData {
	shardsByNamespace := make(map[string][]templates.KeyspaceShardChange, len(cs.Shards))
	var shardedNamespaces []string
	for _, sp := range cs.Shards {
		if sp == nil {
			continue
		}
		shard := templates.KeyspaceShardChange{Shard: sp.GetShard()}
		for _, tc := range sp.GetChanges() {
			if tc.GetDdl() == "" {
				continue
			}
			shard.Statements = append(shard.Statements, tc.GetDdl())
		}
		// A shard with nothing to run already matches the desired schema while
		// its siblings change. It is carried as a satisfied group rather than
		// dropped, so a partially-applied namespace shows its divergent state.
		shard.Satisfied = len(shard.Statements) == 0
		if _, seen := shardsByNamespace[sp.GetNamespace()]; !seen {
			shardedNamespaces = append(shardedNamespaces, sp.GetNamespace())
		}
		shardsByNamespace[sp.GetNamespace()] = append(shardsByNamespace[sp.GetNamespace()], shard)
	}

	changes := make([]templates.KeyspaceChangeData, 0, len(cs.Changes))
	named := make(map[string]bool, len(cs.Changes))
	for _, sc := range cs.Changes {
		if sc == nil {
			continue
		}
		named[sc.GetNamespace()] = true
		ks := templates.KeyspaceChangeData{
			Keyspace: sc.GetNamespace(),
			Shards:   shardsByNamespace[sc.GetNamespace()],
		}
		for _, tc := range sc.GetTableChanges() {
			if tc.GetDdl() == "" {
				continue
			}
			ks.Statements = append(ks.Statements, tc.GetDdl())
		}
		if sc.GetMetadata()[apitypes.VSchemaChangedMetadataKey] == "true" {
			ks.VSchemaChanged = true
			ks.VSchemaDiff = sc.GetMetadata()[apitypes.VSchemaDiffMetadataKey]
		}
		changes = append(changes, ks)
	}
	for _, ns := range shardedNamespaces {
		if named[ns] {
			continue
		}
		changes = append(changes, templates.KeyspaceChangeData{Keyspace: ns, Shards: shardsByNamespace[ns]})
	}
	return changes
}

// describeDriftDiff renders a short, count-based summary of how a diverged
// deployment differs from the reviewed plan, e.g. "1 unexpected, 2 missing
// changes". The full DDL is intentionally omitted to keep the preview compact;
// the operator reconciles by replanning the diverged deployment.
func describeDriftDiff(diff tern.ChangeSetDiff) string {
	var parts []string
	if n := len(diff.UnexpectedInCandidate); n > 0 {
		parts = append(parts, fmt.Sprintf("%d unexpected", n))
	}
	if n := len(diff.MissingFromCandidate); n > 0 {
		parts = append(parts, fmt.Sprintf("%d missing", n))
	}
	if n := len(diff.UnexpectedVSchema); n > 0 {
		parts = append(parts, fmt.Sprintf("%d unexpected vschema", n))
	}
	if n := len(diff.MissingVSchema); n > 0 {
		parts = append(parts, fmt.Sprintf("%d missing vschema", n))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ", ") + " change(s) vs the reviewed plan"
}

// rollupMemberNames renders each rollup entry the way an operator addresses it,
// index-parallel to rollup.Entries.
func rollupMemberNames(rollup api.PlanRollup) []string {
	members := make([]routing.ExecutionTarget, len(rollup.Entries))
	for i, e := range rollup.Entries {
		members[i] = routing.ExecutionTarget{Deployment: e.Deployment, Target: e.Target}
	}
	return routing.DisplayNames(members)
}

// maxDriftSummaryLen bounds the stored drift summary to the checks table's
// change_summary column width. The summary is truncated on a rune boundary so it
// never exceeds the column or splits a multibyte character.
const maxDriftSummaryLen = 255

// summarizeReviewDrift builds the concise operator-facing reason a review-time
// drift rollup blocked the plan check. It names the deployments that diverged
// from the reviewed plan and those that could not be diffed or compared, so the
// check's Change column tells an operator exactly which deployment to reconcile.
func summarizeReviewDrift(rollup api.PlanRollup) string {
	independent := rollup.Planning == api.PlanIndependent
	// One deployment can address several targets, so the deployment name alone
	// does not always say which member failed. The shared naming rule adds the
	// target only where it disambiguates.
	names := rollupMemberNames(rollup)
	var diverged, errored []string
	for i, entry := range rollup.Entries {
		switch entry.Class {
		case api.DeploymentDiverged:
			diverged = append(diverged, names[i])
		case api.DeploymentErrored:
			errored = append(errored, names[i])
		}
	}

	var parts []string
	if len(diverged) > 0 {
		parts = append(parts, fmt.Sprintf("diverged: %s", strings.Join(diverged, ", ")))
	}
	if len(errored) > 0 {
		// An errored member means different things under the two contracts: a
		// mirrored member's diff could not be confirmed against the reviewed plan,
		// while an independent member has no plan of its own at all.
		reason := "could not verify"
		if independent {
			reason = "could not plan"
		}
		parts = append(parts, fmt.Sprintf("%s: %s", reason, strings.Join(errored, ", ")))
	}
	if len(parts) == 0 {
		// A not-clean rollup always has at least one non-passing entry; guard
		// anyway so the check never records an empty, uninformative reason.
		if independent {
			return "blocks apply: not every target could be planned"
		}
		return "drift blocks apply: deployments differ from the reviewed plan"
	}
	// Targets that hold their own schemas are never expected to agree, so their
	// failure is an unplanned target, not drift between them.
	if independent {
		return clampDriftSummary("blocks apply — " + strings.Join(parts, "; "))
	}
	return clampDriftSummary("drift blocks apply — " + strings.Join(parts, "; "))
}

// clampDriftSummary makes a drift summary safe to store in the checks table's
// change_summary column and to render in the aggregate check's markdown: it
// collapses newlines, neutralizes the table cell separator, and truncates on a
// rune boundary to the column width.
func clampDriftSummary(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "|", "/")
	runes := []rune(s)
	if len(runes) > maxDriftSummaryLen {
		return string(runes[:maxDriftSummaryLen-1]) + "…"
	}
	return s
}
