package templates

import (
	"fmt"

	"github.com/block/schemabot/pkg/webhook/action"
)

// PreviewCommentAppScopedDispatch renders a sample app-scoped expansion
// summary: the databases the command targets plus the app databases skipped
// with reasons.
func PreviewCommentAppScopedDispatch() string {
	return RenderAppScopedDispatch(AppScopedDispatchData{
		App:         "billing-service",
		Environment: "staging",
		RequestedBy: previewRequestedBy,
		PinnedSHA:   previewHeadSHA,
		Databases:   []string{"billing-invoices", "billing-ledger"},
		Skipped: []AppScopedSkippedDatabase{
			{Database: "billing-archive", Reason: "environment `staging` is not configured"},
			{Database: "billing-reports", Reason: "no plan for this PR"},
		},
	})
}

// PreviewCommentAppScopedDispatchLarge renders the expansion summary for a
// fleet-sized app, where the database list collapses into a details block
// instead of dominating the PR timeline.
func PreviewCommentAppScopedDispatchLarge() string {
	databases := make([]string, 0, 256)
	for i := 1; i <= 256; i++ {
		databases = append(databases, fmt.Sprintf("tenants-shard-%03d", i))
	}
	return RenderAppScopedDispatch(AppScopedDispatchData{
		App:         "tenants",
		Environment: "production",
		RequestedBy: previewRequestedBy,
		PinnedSHA:   previewHeadSHA,
		Databases:   databases,
		Skipped: []AppScopedSkippedDatabase{
			{Database: "tenants-shard-legacy", Reason: "no plan for this PR"},
		},
	})
}

// PreviewCommentAppScopedDispatchHalted renders a sample mid-dispatch halt:
// the PR head advanced while databases were being dispatched, so the rest
// were not started.
func PreviewCommentAppScopedDispatchHalted() string {
	return RenderAppScopedDispatchHalted(AppScopedDispatchHaltedData{
		App:         "billing-service",
		Environment: "staging",
		CommandName: action.Apply,
		PinnedSHA:   previewHeadSHA,
		CurrentSHA:  previewStaleSHA,
		NotStarted:  []string{"billing-ledger"},
	})
}

// PreviewCommentAppScopedNotAuthorized renders a sample all-or-nothing actor
// authorization denial for an app-scoped command.
func PreviewCommentAppScopedNotAuthorized() string {
	return RenderAppScopedApplyNotAuthorized(AppScopedNotAuthorizedData{
		RequestedBy:        previewRequestedBy,
		CommandName:        action.Apply,
		App:                "billing-service",
		Environment:        "staging",
		Database:           "billing-ledger",
		OperatorPrincipals: []string{"acme/billing-operators"},
		OtherPrincipals:    []string{"acme/db-admins", "jdoe"},
	})
}

// PreviewCommentAppScopedRejected renders a sample fail-closed rejection of an
// app-scoped command whose expansion produced no eligible databases.
func PreviewCommentAppScopedRejected() string {
	return RenderAppScopedExpansionError(AppScopedExpansionErrorData{
		App:         "billing-service",
		Environment: "staging",
		CommandName: action.Apply,
		RequestedBy: previewRequestedBy,
		Reason:      "no database in the app has a stored `staging` plan for this PR — run `schemabot plan -e staging` first",
	})
}

// PreviewCommentAppFlagInvalid renders the usage comment for `--app` with a
// missing or malformed application identifier.
func PreviewCommentAppFlagInvalid() string {
	return RenderInvalidAppFlag(action.Apply)
}

// PreviewCommentAppFlagConflicts renders the rejections for `--app` combined
// with the flags it excludes.
func PreviewCommentAppFlagConflicts() string {
	return "### --app with -d\n\n" +
		RenderAppWithDatabaseFlag(action.Apply) +
		"\n\n### --app with --defer-cutover\n\n" +
		RenderAppDeferCutoverUnsupported() +
		"\n\n### --app on an unsupported command\n\n" +
		RenderUnsupportedAppFlag(action.Plan)
}
