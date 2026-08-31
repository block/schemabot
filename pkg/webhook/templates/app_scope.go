package templates

import (
	"fmt"
	"strings"
)

// RenderInvalidAppFlag renders the usage comment posted when `--app` is
// present without a usable application identifier (missing value, or a value
// that is not alphanumeric with hyphens).
func RenderInvalidAppFlag(command string) string {
	return offerSupportChannel("## Missing or Invalid App\n\n" +
		fmt.Sprintf("Usage: `schemabot %s -e <environment> --app <app>`\n\n", command) +
		"The `--app` flag requires an application identifier (lowercase alphanumeric with hyphens) that matches the `app` field on the target databases.")
}

// RenderUnsupportedAppFlag renders the message posted when `--app` is supplied
// to a command that does not support app scoping.
func RenderUnsupportedAppFlag(command string) string {
	return fmt.Sprintf("The `--app` flag is not supported for `%s`.", command)
}

// RenderAppWithDatabaseFlag renders the rejection posted when a command names
// both an app and a single database. The two flags answer the same question —
// which databases does this command target — so combining them is ambiguous.
func RenderAppWithDatabaseFlag(command string) string {
	return offerSupportChannel("## Conflicting Flags\n\n" +
		"`--app` and `-d` cannot be combined.\n\n" +
		fmt.Sprintf("- `schemabot %s -e <environment> --app <app>` targets every database in the app\n", command) +
		fmt.Sprintf("- `schemabot %s -e <environment> -d <database>` targets a single database\n", command))
}

// RenderAppDeferCutoverUnsupported renders the rejection posted when
// `--defer-cutover` accompanies an app-scoped command. Deferred cutover
// requires a manual cutover command per database, which defeats the purpose of
// a fleet-wide apply; the cautious-rollout path is a single-database canary.
func RenderAppDeferCutoverUnsupported() string {
	return offerSupportChannel("## Conflicting Flags\n\n" +
		"`--defer-cutover` is not supported with `--app`; use `-d` to target individual databases for deferred cutover.\n\n" +
		"To roll out cautiously: canary one database with `-d --defer-cutover`, then apply to the fleet with `--app` (each database cuts over automatically as it finishes copying).")
}

// AppScopedExpansionErrorData carries the fail-closed rejection of an
// app-scoped command before any database was touched. Every field is
// server-controlled; no untrusted error text is rendered.
type AppScopedExpansionErrorData struct {
	App         string
	Environment string
	CommandName string
	RequestedBy string
	// Reason is a server-authored sentence explaining which expansion
	// requirement failed (unknown app, no eligible databases, or a
	// source-policy denial naming the denying database).
	Reason string
}

// RenderAppScopedExpansionError renders the fail-closed rejection posted when
// an app-scoped command cannot expand to a usable database set. No schema
// change was started for any database.
func RenderAppScopedExpansionError(data AppScopedExpansionErrorData) string {
	var sb strings.Builder

	writeEnvironmentTitle(&sb, "⛔ App-Scoped Command Rejected", data.Environment)
	fmt.Fprintf(&sb, "**App**: `%s`\n", data.App)
	if data.RequestedBy != "" {
		fmt.Fprintf(&sb, "**Requested by**: @%s\n", data.RequestedBy)
	}
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "`schemabot %s --app %s` was rejected: %s\n\n", data.CommandName, data.App, data.Reason)
	sb.WriteString("No schema change was started for any database.\n")

	return offerSupportChannel(sb.String())
}

// AppScopedNotAuthorizedData carries the all-or-nothing actor authorization
// denial for an app-scoped command: the actor must be authorized for every
// database in the expansion, and Database names the first one that denied.
type AppScopedNotAuthorizedData struct {
	RequestedBy string
	CommandName string
	App         string
	Environment string
	// Database is the expanded database that denied the actor.
	Database string
	// OperatorPrincipals are the denying database's own operator teams and
	// users; OtherPrincipals are the broader admins also allowed to run
	// mutating commands. Both render as inline code, never @-mentions.
	OperatorPrincipals []string
	OtherPrincipals    []string
}

// RenderAppScopedApplyNotAuthorized renders the denial posted when the actor
// is not authorized for one of the databases an app-scoped command expands to.
// The whole command is denied — no database in the app is applied.
func RenderAppScopedApplyNotAuthorized(data AppScopedNotAuthorizedData) string {
	var sb strings.Builder

	sb.WriteString("## SchemaBot Command Not Authorized\n\n")
	fmt.Fprintf(&sb, "**App**: `%s`\n", data.App)
	writeDBEnvLine(&sb, data.Database, data.Environment)
	sb.WriteString("\n")
	if data.RequestedBy != "" {
		fmt.Fprintf(&sb, "@%s is not authorized to run `schemabot %s` for database `%s`.\n\n", data.RequestedBy, data.CommandName, data.Database)
	} else {
		fmt.Fprintf(&sb, "The requester is not authorized to run `schemabot %s` for database `%s`.\n\n", data.CommandName, data.Database)
	}
	fmt.Fprintf(&sb, "An app-scoped command requires authorization for **every** database in the app, so the whole command was denied — no database in `%s` was applied.\n\n", data.App)

	hasOperators := len(data.OperatorPrincipals) > 0
	hasOthers := len(data.OtherPrincipals) > 0
	switch {
	case hasOperators:
		fmt.Fprintf(&sb, "**Operators of `%s`** — members of these teams, or these users, can run it:\n", data.Database)
		writePrincipalList(&sb, data.OperatorPrincipals)
		if hasOthers {
			sb.WriteString("\n**Other authorized teams and users**:\n")
			writePrincipalList(&sb, data.OtherPrincipals)
		}
		writeAskPrincipalsGuidance(&sb, data.OperatorPrincipals, data.OtherPrincipals)
	case hasOthers:
		sb.WriteString("**Who can run this command** — members of these teams, or these users:\n")
		writePrincipalList(&sb, data.OtherPrincipals)
		writeAskPrincipalsGuidance(&sb, data.OtherPrincipals)
	default:
		sb.WriteString("A configured SchemaBot admin/database operator must run this command.\n")
	}

	return offerSupportChannel(sb.String())
}

// AppScopedDispatchHaltedData carries the mid-dispatch halt of an app-scoped
// command: a new commit landed on the PR after some databases already started,
// so the remaining databases were not started at the now-superseded commit.
type AppScopedDispatchHaltedData struct {
	App         string
	Environment string
	CommandName string
	// PinnedSHA is the PR head the dispatch started from; CurrentSHA is the
	// newer head observed mid-dispatch.
	PinnedSHA  string
	CurrentSHA string
	// NotStarted are the databases that were not dispatched, in name order.
	NotStarted []string
}

// RenderAppScopedDispatchHalted renders the halt notice posted when the PR
// head advances while an app-scoped command is dispatching its databases.
// Databases already started continue at the pinned commit and report through
// their own comments; the rest wait for a fresh command against the new head.
func RenderAppScopedDispatchHalted(data AppScopedDispatchHaltedData) string {
	var sb strings.Builder

	writeEnvironmentTitle(&sb, "⚠️ App-Scoped Dispatch Halted", data.Environment)
	fmt.Fprintf(&sb, "**App**: `%s`\n\n", data.App)
	fmt.Fprintf(&sb, "A new commit landed on this PR while `schemabot %s --app %s` was dispatching: the command started from `%s`, but the PR head is now `%s`.\n\n",
		data.CommandName, data.App, data.PinnedSHA, data.CurrentSHA)
	subject := "these databases were"
	if len(data.NotStarted) == 1 {
		subject = "this database was"
	}
	fmt.Fprintf(&sb, "To keep every database on the same commit, %s **not** started:\n\n", subject)
	for _, db := range data.NotStarted {
		fmt.Fprintf(&sb, "- `%s`\n", db)
	}
	fmt.Fprintf(&sb, "\nDatabases dispatched before the halt continue at `%s` and report in their own comments. Review the new commit, then run `schemabot %s -e %s --app %s` again to apply the current head everywhere.\n",
		data.PinnedSHA, data.CommandName, data.Environment, data.App)

	return offerSupportChannel(sb.String())
}

// AppScopedSkippedDatabase is a database in the app that is not part of this
// apply, with the server-authored reason it was left out.
type AppScopedSkippedDatabase struct {
	Database string
	Reason   string
}

// AppScopedDispatchData carries the expansion summary posted once an
// app-scoped command passed every fail-closed gate and is about to run per
// database.
type AppScopedDispatchData struct {
	App         string
	Environment string
	CommandName string
	RequestedBy string
	// Databases are the expanded databases the command will run against, in
	// name order.
	Databases []string
	// Skipped are app databases excluded from this run, with reasons.
	Skipped []AppScopedSkippedDatabase
}

// RenderAppScopedDispatch renders the expansion summary posted before an
// app-scoped command runs against each database. Each database then gets its
// own plan/progress comments, exactly as a single-database command would.
func RenderAppScopedDispatch(data AppScopedDispatchData) string {
	var sb strings.Builder

	writeEnvironmentTitle(&sb, "App-Scoped Apply", data.Environment)
	fmt.Fprintf(&sb, "**App**: `%s`\n", data.App)
	if data.RequestedBy != "" {
		fmt.Fprintf(&sb, "**Requested by**: @%s\n", data.RequestedBy)
	}
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "`schemabot %s --app %s` expands to **%d** %s:\n\n",
		data.CommandName, data.App, len(data.Databases), pluralize("database", len(data.Databases)))
	for _, db := range data.Databases {
		fmt.Fprintf(&sb, "- `%s`\n", db)
	}
	if len(data.Skipped) > 0 {
		sb.WriteString("\n**Skipped**:\n")
		for _, skipped := range data.Skipped {
			fmt.Fprintf(&sb, "- `%s` — %s\n", skipped.Database, skipped.Reason)
		}
	}
	sb.WriteString("\nEach database runs as its own apply with its own progress comment and check.\n")

	return sb.String()
}
