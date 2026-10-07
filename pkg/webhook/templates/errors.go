package templates

import (
	"fmt"
	"html"
	"strings"
	"text/template"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

// SchemaErrorData contains data for rendering schema request error comments.
type SchemaErrorData struct {
	// ExperimentalStrataEnabled is set only from the server configuration.
	ExperimentalStrataEnabled bool

	// RequestedBy is the GitHub login that issued the command. Empty means the
	// command was system-triggered (an auto-plan from a pull request update).
	RequestedBy string
	Timestamp   string
	// Environment is the single environment the command targeted. Empty means
	// the command was not scoped to one environment: multi-environment plans
	// (including auto-plans) target every configured environment.
	Environment string
	// Environments is the set of environments this SchemaBot deployment
	// handles, rendered when Environment is empty. In a multi-deployment
	// topology each deployment is scoped to its own environments, so this
	// names the concrete environments the failed command covered. Empty means
	// the deployment is unscoped and the header omits the environment segment
	// rather than rendering filler.
	Environments       []string
	DatabaseName       string
	SchemaPath         string
	CommandName        string // "plan" or "apply"
	ErrorDetail        string
	AvailableDatabases string
	// SearchedDirs lists the schema directories a database-scoped search was
	// limited to when GitHub truncated the repository tree. Empty when the
	// whole repository was searched.
	SearchedDirs []string
	// UnregisteredConfigs lists the schema configs whose database this
	// deployment has not registered and whose schema directory is under no
	// path it expects another deployment to report on.
	UnregisteredConfigs []UnregisteredSchemaConfigData
	// Deployment names the SchemaBot deployment making a claim about its own
	// registry: the one environment it serves. Empty when it serves several
	// or every environment, and the comment then renders the environment
	// header instead.
	Deployment string
	// DeploymentEnvironments names, in promotion order, the environments a
	// deployment serving several but not every environment speaks for, so a
	// claim about its registry says which environments it covers. Empty for a
	// deployment serving one environment (Deployment names it) or every one.
	DeploymentEnvironments []string
}

// UnregisteredSchemaConfigData identifies one schema config whose database the
// deployment posting the comment has not registered.
type UnregisteredSchemaConfigData struct {
	Database   string
	SchemaPath string
}

// MultipleUnregisteredConfigs reports whether the error names more than one
// unregistered schema config, which renders them as a list.
func (d SchemaErrorData) MultipleUnregisteredConfigs() bool {
	return len(d.UnregisteredConfigs) > 1
}

// UnregisteredConfigLines renders each unregistered schema config as its
// schema directory and the database it declares, as code spans the
// PR-supplied values cannot break out of.
func (d SchemaErrorData) UnregisteredConfigLines() []string {
	lines := make([]string, 0, len(d.UnregisteredConfigs))
	for _, cfg := range d.UnregisteredConfigs {
		lines = append(lines, inlineCode(cfg.SchemaPath)+" declares database "+inlineCode(cfg.Database))
	}
	return lines
}

// DeploymentHeader renders the header segment naming the deployment that
// makes the claim, falling back to the environment header when the
// deployment serves more than one environment.
func (d SchemaErrorData) DeploymentHeader() string {
	if d.Deployment != "" {
		return "**Deployment**: " + inlineCode(d.Deployment)
	}
	return d.EnvironmentHeader()
}

// DeploymentSubject names the deployment making the claim as the subject of a
// sentence.
func (d SchemaErrorData) DeploymentSubject() string {
	switch {
	case d.Deployment != "":
		return "The " + flattenIdentifier(d.Deployment) + " SchemaBot deployment"
	case len(d.DeploymentEnvironments) > 0:
		return "The SchemaBot deployment serving " + joinWithAnd(inlineCodeList(d.DeploymentEnvironments))
	default:
		return "This SchemaBot deployment"
	}
}

// DeploymentSubjectLower is DeploymentSubject for use mid-sentence.
func (d SchemaErrorData) DeploymentSubjectLower() string {
	switch {
	case d.Deployment != "":
		return "the " + flattenIdentifier(d.Deployment) + " SchemaBot deployment"
	case len(d.DeploymentEnvironments) > 0:
		return "the SchemaBot deployment serving " + joinWithAnd(inlineCodeList(d.DeploymentEnvironments))
	default:
		return "this SchemaBot deployment"
	}
}

// SearchedDirsCode renders the directories a scoped search probed as code
// spans the paths cannot break out of.
func (d SchemaErrorData) SearchedDirsCode() []string {
	return inlineCodeList(d.SearchedDirs)
}

// DatabaseTypeOptions lists supported setup types, with Strata behind server opt-in.
func (d SchemaErrorData) DatabaseTypeOptions() string {
	if d.ExperimentalStrataEnabled {
		return "`mysql`, `postgres`, `vitess`, or `strata` (experimental)"
	}
	return "`mysql`, `postgres`, or `vitess`"
}

// DatabaseNameCode renders the database the command or the PR's config named
// as a code span the name cannot break out of.
func (d SchemaErrorData) DatabaseNameCode() string {
	return inlineCode(d.DatabaseName)
}

// DatabaseDeclarationCode renders the schemabot.yaml line that would declare
// the database the command named, as a code span the name cannot break out of.
func (d SchemaErrorData) DatabaseDeclarationCode() string {
	return inlineCode("database: " + d.DatabaseName)
}

// SetupConfigBlock renders a starter schemabot.yaml for the database the
// command named, inside a fence the name cannot close early; the name is kept
// to one line so the starter stays one declaration per line.
func (d SchemaErrorData) SetupConfigBlock() string {
	var sb strings.Builder
	writeFencedBlock(&sb, "yaml", "database: "+flattenIdentifier(d.DatabaseName)+"\ntype: mysql")
	return sb.String()
}

// SchemaPathCode renders the schema directory the PR's config declared as a
// code span the path cannot break out of.
func (d SchemaErrorData) SchemaPathCode() string {
	return inlineCode(d.SchemaPath)
}

// AllowedDirsKey renders the server config key that would authorize the
// database's schema directory, as one code span the database name cannot
// break out of.
func (d SchemaErrorData) AllowedDirsKey() string {
	return allowedDirsKey(d.DatabaseName)
}

func allowedDirsKey(database string) string {
	return inlineCode("databases." + database + ".allowed_dirs")
}

// EnvironmentHeader renders the environment header segment: the single
// environment the command targeted, or the deployment's environment scope
// when the command spanned environments (multi-environment plans, including
// auto-plans). Returns "" when neither is known so templates drop the
// segment — never an empty code span or a vague placeholder.
func (d SchemaErrorData) EnvironmentHeader() string {
	if d.Environment != "" {
		return "**Environment**: " + inlineCode(d.Environment)
	}
	switch len(d.Environments) {
	case 0:
		return ""
	case 1:
		return "**Environment**: " + inlineCode(d.Environments[0])
	default:
		return "**Environments**: " + strings.Join(inlineCodeList(d.Environments), ", ")
	}
}

// ExampleEnvironment is the -e value rendered inside pasteable usage
// examples: the requested environment when one was given, the deployment's
// sole environment when it is scoped to exactly one, otherwise a placeholder
// for the reader to fill in.
func (d SchemaErrorData) ExampleEnvironment() string {
	if d.Environment != "" {
		return d.Environment
	}
	if len(d.Environments) == 1 {
		return d.Environments[0]
	}
	return "<environment>"
}

// Attribution renders the footer attribution line: the requesting user for
// user-issued commands, or the automatic trigger for system-issued ones —
// never a bare @ mention.
func (d SchemaErrorData) Attribution() string {
	if d.RequestedBy == "" {
		return "*Triggered automatically by a pull request update at " + d.Timestamp + " UTC*"
	}
	return "*Requested by @" + d.RequestedBy + " at " + d.Timestamp + " UTC*"
}

// SchemaConfigDocs renders the documentation link for the repository-side
// schemabot.yaml. Every comment a first-time user can reach before they have a
// working config carries it: without one the comment states a requirement and
// leaves the reader to guess the rest of the file (UX-4).
func (d SchemaErrorData) SchemaConfigDocs() string {
	return glyph.Docs + " **Docs:** [Setting up `schemabot.yaml`](" + ui.SchemaConfigDocURL + ")"
}

const databaseNotFoundTemplate = "## " + glyph.Attention + ` Database Not Found

**Database**: {{.DatabaseNameCode}}{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

{{if .SearchedDirsCode}}No ` + "`schemabot.yaml`" + ` configuration with {{.DatabaseDeclarationCode}} was found in the schema directories configured for this database on the SchemaBot server:

{{range .SearchedDirsCode}}- {{.}}
{{end}}
This repository is too large for GitHub to return its full tree, so SchemaBot searched only those directories. Check that the ` + "`schemabot.yaml`" + ` for this database lives under one of them and that its ` + "`database`" + ` field matches the ` + "`-d`" + ` flag value.{{else}}No ` + "`schemabot.yaml`" + ` configuration with {{.DatabaseDeclarationCode}} was found in this repository.

Check that your ` + "`schemabot.yaml`" + ` file has the correct ` + "`database`" + ` field matching the ` + "`-d`" + ` flag value.{{end}}

{{.SchemaConfigDocs}}`

const databaseNotConfiguredTemplate = "## " + glyph.Attention + ` Database Not Configured

**Database**: {{.DatabaseNameCode}}{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

This SchemaBot instance has no {{.DatabaseNameCode}} entry under ` + "`databases`" + ` in its server configuration, so it cannot plan or apply schema changes for it. A ` + "`schemabot.yaml`" + ` declaring {{.DatabaseDeclarationCode}} is not enough on its own: the database also has to be configured on the SchemaBot server.

Check that the database name, from ` + "`-d`" + ` or from ` + "`schemabot.yaml`" + `, matches one this instance serves, or ask a SchemaBot operator to configure the database.`

const databaseNotRegisteredTemplate = "## " + glyph.Attention + ` {{if .MultipleUnregisteredConfigs}}Databases Not Registered{{else}}Database Not Registered{{end}}

{{if .MultipleUnregisteredConfigs}}{{with .DeploymentHeader}}{{.}}

{{end}}{{else}}**Database**: {{.DatabaseNameCode}} | **Schema directory**: {{.SchemaPathCode}}{{with .DeploymentHeader}} | {{.}}{{end}}

{{end}}{{.Attribution}}

{{if .MultipleUnregisteredConfigs}}{{.DeploymentSubject}} has none of these databases under ` + "`databases`" + `, and none of these schema directories is under a path it expects another deployment to report on:

{{range .UnregisteredConfigLines}}- {{.}}
{{end}}
For each database that is new to SchemaBot, ask a SchemaBot operator to register it with its schema directory. For each one already registered, move its ` + "`schemabot.yaml`" + ` and schema files under the schema directory registered for it.{{else}}{{.DeploymentSubject}} has no {{.DatabaseNameCode}} entry under ` + "`databases`" + `, and this schema directory is not under any path it expects another deployment to report on.

If {{.DatabaseNameCode}} is new to SchemaBot, ask a SchemaBot operator to register it with this schema directory. If it is already registered, move the ` + "`schemabot.yaml`" + ` and its schema files under the schema directory registered for it.{{end}}`

const databaseRepoNotAllowedTemplate = "## " + glyph.Attention + ` Database Not Available to This Repository

**Database**: {{.DatabaseNameCode}}{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

The SchemaBot server configures {{.DatabaseNameCode}} to accept schema changes from other repositories only: this repository is not in the database's ` + "`allowed_repos`" + `, so no ` + "`schemabot.yaml`" + ` in it can manage the database and none was searched.

Ask a SchemaBot operator to add this repository to the database's ` + "`allowed_repos`" + ` if it should manage the database, or check that the database name, from ` + "`-d`" + ` or from ` + "`schemabot.yaml`" + `, names the right database.`

const repositoryTreeTruncatedTemplate = "## " + glyph.Attention + ` Repository Too Large to Search

{{if .DatabaseName}}**Database**: {{.DatabaseNameCode}}{{with .EnvironmentHeader}} | {{.}}{{end}}

{{else}}{{with .EnvironmentHeader}}{{.}}

{{end}}{{end}}{{.Attribution}}

GitHub returned a truncated repository tree, so SchemaBot could not search this repository for ` + "`schemabot.yaml`" + ` configurations. On a repository this large, SchemaBot searches only the schema directories configured on the SchemaBot server{{if .DatabaseName}}, and it has none it can search exhaustively for {{.DatabaseNameCode}}: the database is not configured on this instance, or its ` + "`allowed_dirs`" + ` leave the location of its config open{{else}} for this repository's databases, and those do not bound where every config may live{{end}}.

{{if .DatabaseName}}Ask a SchemaBot operator to configure the database with an ` + "`allowed_dirs`" + ` entry naming its schema directory, or check that the database name, from ` + "`-d`" + ` or from ` + "`schemabot.yaml`" + `, matches one this instance serves.{{else}}Ask a SchemaBot operator to give each of this repository's databases an ` + "`allowed_dirs`" + ` entry naming its schema directory.{{end}}`

const invalidConfigTemplate = "## " + glyph.Attention + ` No Valid SchemaBot Configuration Found

{{with .EnvironmentHeader}}{{.}}

{{end}}{{.Attribution}}

The ` + "`schemabot.yaml`" + ` file must include ` + "`database`" + ` and ` + "`type`" + ` fields:

` + "```yaml" + `
database: your-database-name
type: mysql
` + "```" + `

- **database** (required): The database name
- **type** (required): {{.DatabaseTypeOptions}}

{{.SchemaConfigDocs}}`

const noConfigNoDatabaseTemplate = "## " + glyph.Info + ` No SchemaBot Configuration Found

{{with .EnvironmentHeader}}{{.}}

{{end}}{{.Attribution}}

No ` + "`schemabot.yaml`" + ` configuration file was found in this repository.

### Setup Instructions
Create a ` + "`schemabot.yaml`" + ` file in the directory holding the ` + "`.sql`" + ` files that declare your tables:

` + "```yaml" + `
database: your-database-name
type: mysql
` + "```" + `

` + "`type`" + `: {{.DatabaseTypeOptions}}

{{.SchemaConfigDocs}}

### If you already have a config
Use the ` + "`-d`" + ` flag to specify which database to {{.CommandName}}:

` + "```" + `
schemabot {{.CommandName}} -e {{.ExampleEnvironment}} -d <database-name>
` + "```" + ``

const noConfigWithDatabaseTemplate = "## " + glyph.Info + ` No SchemaBot Configuration Found

**Database**: {{.DatabaseNameCode}}{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

No ` + "`schemabot.yaml`" + ` configuration file exists in this repository.

### Setup Instructions
Create a ` + "`schemabot.yaml`" + ` file in the directory holding the ` + "`.sql`" + ` files that declare your tables:

{{.SetupConfigBlock}}
` + "`type`" + `: {{.DatabaseTypeOptions}}

{{.SchemaConfigDocs}}`

const configOutsideAllowedDirsTemplate = "## " + glyph.Attention + ` SchemaBot Configuration Not Authorized

**Database**: {{.DatabaseNameCode}}{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

SchemaBot found a ` + "`schemabot.yaml`" + ` configuration, but {{.DeploymentSubjectLower}} is not configured to manage its schema directory.

**Schema directory**: {{.SchemaPathCode}}

Ask a SchemaBot operator to add this directory to {{.AllowedDirsKey}} in the server config, or move the schema config and files under an allowed directory.`

const unmanagedSchemaConfigsNoticeTemplate = "## " + glyph.Attention + ` Schema Changes Not Managed by SchemaBot
{{with .Environments}}
**Environments**: {{.}}
{{end}}
This PR changes schema under the following path(s), which SchemaBot is not configured to manage in any environment:

{{range .Configs}}- {{.SchemaPath}} — declares database {{.Database}}
{{end}}
These schema changes will **not** be planned or applied in any environment, and the SchemaBot checks on this PR do not cover them.

If SchemaBot should manage them, ask a SchemaBot operator to add the directory to the database's ` + "`allowed_dirs`" + ` in the server config; otherwise remove these schema changes from this PR.`

const multipleConfigsTemplate = "## " + glyph.Attention + ` Multiple Databases Detected

{{with .EnvironmentHeader}}{{.}}

{{end}}{{.Attribution}}

This repository has multiple ` + "`schemabot.yaml`" + ` configurations.

### Available Databases

{{.AvailableDatabases}}

### How to specify a database

Use the ` + "`-d`" + ` flag:

` + "```" + `
schemabot {{.CommandName}} -e {{.ExampleEnvironment}} -d <database-name>
` + "```" + ``

const genericErrorTemplate = "## " + glyph.Failed + ` {{.CommandName}} Failed

{{with .EnvironmentHeader}}{{.}}

{{end}}{{.Attribution}}

### Error

> {{.ErrorDetail}}`

// Compiled templates.
var (
	tmplDatabaseNotFound      = template.Must(template.New("databaseNotFound").Parse(databaseNotFoundTemplate))
	tmplDatabaseNotConfig     = template.Must(template.New("databaseNotConfigured").Parse(databaseNotConfiguredTemplate))
	tmplDatabaseNotRegistered = template.Must(template.New("databaseNotRegistered").Parse(databaseNotRegisteredTemplate))
	tmplRepoTreeTruncated     = template.Must(template.New("repositoryTreeTruncated").Parse(repositoryTreeTruncatedTemplate))
	tmplDatabaseRepoDenied    = template.Must(template.New("databaseRepoNotAllowed").Parse(databaseRepoNotAllowedTemplate))
	tmplInvalidConfig         = template.Must(template.New("invalidConfig").Parse(invalidConfigTemplate))
	tmplNoConfigNoDatabase    = template.Must(template.New("noConfigNoDatabase").Parse(noConfigNoDatabaseTemplate))
	tmplNoConfigWithDatabase  = template.Must(template.New("noConfigWithDatabase").Parse(noConfigWithDatabaseTemplate))
	tmplConfigNotAuthorized   = template.Must(template.New("configOutsideAllowedDirs").Parse(configOutsideAllowedDirsTemplate))
	tmplUnmanagedNotice       = template.Must(template.New("unmanagedSchemaConfigsNotice").Parse(unmanagedSchemaConfigsNoticeTemplate))
	tmplMultipleConfigs       = template.Must(template.New("multipleConfigs").Parse(multipleConfigsTemplate))
	tmplGenericError          = template.Must(template.New("genericError").Parse(genericErrorTemplate))
)

// RenderDatabaseNotFound renders the "database not found" error comment. When
// SearchedDirs is set, the search was limited to those directories because the
// repository tree was truncated, and the comment says so instead of claiming
// the whole repository was searched.
func RenderDatabaseNotFound(data SchemaErrorData) string {
	return offerSupportChannel(renderTemplate(tmplDatabaseNotFound, data))
}

// RenderDatabaseNotConfigured renders the error shown when a command names a
// database this SchemaBot instance has no server-side configuration for. It is
// distinct from Database Not Found: the repository may well hold a correct
// schemabot.yaml, and the remedy is server-side.
func RenderDatabaseNotConfigured(data SchemaErrorData) string {
	return offerSupportChannel(renderTemplate(tmplDatabaseNotConfig, data))
}

// RenderDatabaseNotRegistered renders the error the aggregate leader posts
// when the PR's schema config declares a database its own registry lacks and
// sits under no path it expects another deployment to report on. The claim is
// scoped to the deployment posting it: another deployment on the repository
// may still register the database. data.UnregisteredConfigs names every such
// config; one config renders in the header, several as a list.
func RenderDatabaseNotRegistered(data SchemaErrorData) string {
	if len(data.UnregisteredConfigs) == 1 {
		data.DatabaseName = data.UnregisteredConfigs[0].Database
		data.SchemaPath = data.UnregisteredConfigs[0].SchemaPath
	}
	return offerSupportChannel(renderTemplate(tmplDatabaseNotRegistered, data))
}

// RenderRepositoryTreeTruncated renders the error shown when GitHub truncated
// the repository tree and the server-side schema directories could not bound
// the search, so config discovery failed closed.
func RenderRepositoryTreeTruncated(data SchemaErrorData) string {
	return offerSupportChannel(renderTemplate(tmplRepoTreeTruncated, data))
}

// RenderDatabaseRepoNotAllowed renders the error comment for a database-scoped
// command naming a database the SchemaBot server configures, but whose
// allowed_repos exclude this repository.
func RenderDatabaseRepoNotAllowed(data SchemaErrorData) string {
	return offerSupportChannel(renderTemplate(tmplDatabaseRepoDenied, data))
}

// RenderInvalidConfig renders the "invalid config" error comment.
func RenderInvalidConfig(data SchemaErrorData) string {
	return offerSupportChannel(renderTemplate(tmplInvalidConfig, data))
}

// RenderNoConfig renders the "no config found" error comment.
func RenderNoConfig(data SchemaErrorData) string {
	if data.DatabaseName == "" {
		return renderTemplate(tmplNoConfigNoDatabase, data)
	}
	return renderTemplate(tmplNoConfigWithDatabase, data)
}

// RenderConfigNotAuthorizedLine renders the same explanation as
// RenderConfigNotAuthorized on one line, for surfaces that carry a command's
// failure as an error string rather than a comment of its own. The schema
// directory and database name come from the PR's own config, so both render
// as code spans they cannot break out of.
func RenderConfigNotAuthorizedLine(database, schemaPath string) string {
	return strings.Join([]string{
		"SchemaBot found a `schemabot.yaml` configuration, but this SchemaBot deployment is not configured to manage its schema directory.",
		"Schema directory: " + inlineCode(schemaPath) + ".",
		"Ask a SchemaBot operator to add this directory to " + allowedDirsKey(database) + " in the server config, or move the schema config and files under an allowed directory.",
	}, " ")
}

// RenderConfigNotAuthorized renders the error shown when schemabot.yaml exists
// but its schema directory is outside the server-side allowed_dirs boundary.
func RenderConfigNotAuthorized(data SchemaErrorData) string {
	return offerSupportChannel(renderTemplate(tmplConfigNotAuthorized, data))
}

// UnmanagedSchemaConfigNoticeData identifies one schema config discovered in
// a PR that this deployment is not configured to manage.
type UnmanagedSchemaConfigNoticeData struct {
	Database   string
	SchemaPath string
}

// RenderUnmanagedSchemaConfigsNotice renders the notice posted when a PR
// changes schema under configs this deployment is not authorized to manage
// and no other deployment is expected to handle them. Only a deployment
// serving every environment posts it, so the claim covers every environment,
// and environments names them in promotion order; the header is omitted when
// none are known. The database names and paths come from the PR's own
// schemabot.yaml files — untrusted input — so each is normalized into a safe
// inline code span before rendering.
func RenderUnmanagedSchemaConfigsNotice(environments []string, configs []UnmanagedSchemaConfigNoticeData) string {
	normalized := make([]UnmanagedSchemaConfigNoticeData, len(configs))
	for i, cfg := range configs {
		normalized[i] = UnmanagedSchemaConfigNoticeData{
			Database:   inlineCode(cfg.Database),
			SchemaPath: inlineCode(cfg.SchemaPath),
		}
	}
	var sb strings.Builder
	data := struct {
		Environments string
		Configs      []UnmanagedSchemaConfigNoticeData
	}{Environments: strings.Join(inlineCodeList(environments), ", "), Configs: normalized}
	if err := tmplUnmanagedNotice.Execute(&sb, data); err != nil {
		return fmt.Sprintf("Error rendering template: %v", err)
	}
	return offerSupportChannel(sb.String())
}

// RenderUnmanagedSchemaPassingCheck renders the title and summary of the
// passing aggregate Check Run for a PR whose schema changes all sit under
// configs this deployment does not manage. "No schema files changed" would be
// false there: the PR does change schema, just none this deployment plans.
// environment names the environment the Check Run covers, empty for a
// deployment serving every environment. The database names and paths come
// from the PR's own schemabot.yaml files, so each renders as a code span it
// cannot break out of.
func RenderUnmanagedSchemaPassingCheck(environment string, configs []UnmanagedSchemaConfigNoticeData) (title, summary string) {
	scope := "in any environment"
	title = "No schema changes managed by SchemaBot"
	if environment != "" {
		scope = "in " + inlineCode(environment)
		title = "No schema changes managed in " + flattenIdentifier(environment)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "This PR changes schema only under paths SchemaBot does not manage %s, so there is nothing to plan or apply %s:\n\n", scope, scope)
	for _, cfg := range configs {
		fmt.Fprintf(&sb, "- %s declares database %s\n", inlineCode(cfg.SchemaPath), inlineCode(cfg.Database))
	}
	return title, sb.String()
}

// RenderUnmanagedSchemaPlanNote renders the note a plan comment carries when
// the PR also changes schema under configs this deployment does not manage.
// An environment-scoped deployment posts no separate notice, since a sibling
// serving another environment may manage those configs, so the plan comment
// is where the PR shows that this deployment left them out. environments
// names the environments this deployment serves, and the note makes no claim
// about any other. It renders nothing when configs is empty. The database
// names and paths come from the PR's own schemabot.yaml files, so each renders
// as a code span it cannot break out of.
func RenderUnmanagedSchemaPlanNote(environments []string, configs []UnmanagedSchemaConfigNoticeData) string {
	if len(configs) == 0 {
		return ""
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s This PR also changes schema under paths SchemaBot does not manage in %s, so this plan does not cover them:\n\n",
		glyph.Info, joinWithAnd(inlineCodeList(environments)))
	for _, cfg := range configs {
		fmt.Fprintf(&sb, "- %s declares database %s\n", inlineCode(cfg.SchemaPath), inlineCode(cfg.Database))
	}
	return sb.String()
}

// PreviewCommentUnmanagedSchemaConfigsNotice renders a sample notice for
// schema changes under configs this deployment does not manage, with the
// support-channel footer a configured deployment appends to it.
func PreviewCommentUnmanagedSchemaConfigsNotice() string {
	return RenderSupportChannelFooter(RenderUnmanagedSchemaConfigsNotice([]string{"staging", "production"}, []UnmanagedSchemaConfigNoticeData{
		{Database: "inventory", SchemaPath: "services/inventory/schema"},
	}), previewSupportChannel())
}

// RenderMultipleConfigs renders the "multiple configs" error comment.
func RenderMultipleConfigs(data SchemaErrorData) string {
	return offerSupportChannel(renderTemplate(tmplMultipleConfigs, data))
}

// RenderGenericError renders a generic error comment.
func RenderGenericError(data SchemaErrorData) string {
	// Capitalize command name for header
	data.CommandName = capitalizeFirst(data.CommandName)
	// The error detail is untrusted engine or infrastructure error text:
	// sanitize it, escape HTML, and keep a multi-line message inside the
	// blockquote so it cannot leak endpoints, inject markup, or escape into
	// the surrounding comment structure.
	data.ErrorDetail = quoteBlockLines(html.EscapeString(sanitizeCommentError(data.ErrorDetail)))
	return offerSupportChannel(renderTemplate(tmplGenericError, data))
}

// RenderInvalidCommand generates an error message for unrecognized commands.
func RenderInvalidCommand() string {
	return offerSupportChannel("## " + glyph.Failed + " Invalid Command\n\nThat command wasn't recognized. Available commands:\n\n" + commandReference())
}

// RenderInvalidEnv generates an error message when the -e value does not name
// a configured environment — whether malformed (e.g. a flag glued onto the
// value by a missing space) or simply not an environment any instance
// handles. The configured environment names are normalized for markdown
// display so an unexpected character cannot break the comment.
func RenderInvalidEnv(action string, available []string) string {
	quoted := inlineCodeList(available)
	availableLine := ""
	if len(quoted) > 0 {
		availableLine = "\n**Available environments**: " + strings.Join(quoted, ", ") + "\n"
	}
	return offerSupportChannel(fmt.Sprintf("## "+glyph.Failed+` Invalid Environment

`+"`-e`"+` must name one of the configured environments.
%s
**Usage**: `+"`schemabot %s -e <environment> [flags]`", availableLine, action))
}

// inlineCodeList renders each value as a code span, ready to join into a
// comma-separated list.
func inlineCodeList(values []string) []string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = inlineCode(v)
	}
	return quoted
}

// RenderMissingEnv generates an error message when -e flag is missing.
func RenderMissingEnv(action string) string {
	return offerSupportChannel(fmt.Sprintf("## "+glyph.Failed+` Missing Argument

You'll need to specify which environment to target with the `+"`-e`"+` flag.

**Usage**: `+"`schemabot %s -e <environment>`"+`

**Example**:
`+"```"+`
schemabot %s -e staging
`+"```", action, action))
}

func renderTemplate(tmpl *template.Template, data SchemaErrorData) string {
	var sb strings.Builder
	if err := tmpl.Execute(&sb, data); err != nil {
		return fmt.Sprintf("Error rendering template: %v", err)
	}
	return sb.String()
}

// FormatAvailableDatabases formats database names from error message as markdown list.
func FormatAvailableDatabases(errMsg string) string {
	// Error message format: "multiple schemabot.yaml configs found...: `db1` (path1), `db2` (path2)"
	colonIdx := strings.LastIndex(errMsg, ": ")
	if colonIdx == -1 || colonIdx+2 >= len(errMsg) {
		return "- (Unable to determine available databases)"
	}

	databasesPart := errMsg[colonIdx+2:]
	parts := strings.Split(databasesPart, ", ")

	var result strings.Builder
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			fmt.Fprintf(&result, "- %s\n", part)
		}
	}

	if result.Len() == 0 {
		return "- (Unable to determine available databases)"
	}
	return result.String()
}

// MemberPlanBlockedDetail is the error line for an apply whose creation refused
// one rollout target's own plan for a change its engine refuses. It is built
// from the target's name and the table rather than from the refusal's error
// text, and it names the target so the operator looks for the change under
// that target's plan instead of in the primary plan.
func MemberPlanBlockedDetail(target, table string) string {
	return fmt.Sprintf("Target %s has a change on table %s that its engine refuses to execute, so nothing was applied. Fix what that target's plan names as the reason, then run the command again.",
		inlineCode(target), inlineCode(table))
}

// MemberPlanUnsafeWithoutOptInDetail is the error line for an apply whose
// creation refused one rollout target's own plan for an unsafe change, on an
// apply created without `--allow-unsafe`. Like MemberPlanBlockedDetail, it is
// built from names SchemaBot controls; table is empty for a VSchema change in
// namespace.
func MemberPlanUnsafeWithoutOptInDetail(target, table, namespace string) string {
	subject := "table " + inlineCode(table)
	if table == "" {
		subject = "the VSchema of namespace " + inlineCode(namespace)
	}
	return fmt.Sprintf("Target %s has an unsafe change on %s, so nothing was applied. Run the command again with `--allow-unsafe` to apply it.",
		inlineCode(target), subject)
}

// MemberPlanUndisclosedUnsafeDetail is the error line for an apply whose
// creation refused one rollout target's own plan for an unsafe change the
// comment behind the apply did not show: it showed one plan, which does not
// carry the change. No `--allow-unsafe` covers a change the comment never
// disclosed, so the line does not suggest one, and points instead at a fresh
// apply, whose comment shows each target's own plan. Like
// MemberPlanBlockedDetail, it is built from names SchemaBot controls; table is
// empty for a VSchema change in namespace.
func MemberPlanUndisclosedUnsafeDetail(target, table, namespace string) string {
	subject := "table " + inlineCode(table)
	if table == "" {
		subject = "the VSchema of namespace " + inlineCode(namespace)
	}
	return fmt.Sprintf("Target %s has an unsafe change on %s that the plan comment never disclosed, so `--allow-unsafe` cannot consent to it. Nothing was applied. Run apply again for this environment to review each target's own plan.",
		inlineCode(target), subject)
}
