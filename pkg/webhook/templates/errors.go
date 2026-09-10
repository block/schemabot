package templates

import (
	"fmt"
	"html"
	"strings"
	"text/template"

	"github.com/block/schemabot/pkg/glyph"
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

const databaseNotFoundTemplate = "## " + glyph.Attention + ` Database Not Found

**Database**: {{.DatabaseNameCode}}{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

{{if .SearchedDirsCode}}No ` + "`schemabot.yaml`" + ` configuration with {{.DatabaseDeclarationCode}} was found in the schema directories configured for this database on the SchemaBot server:

{{range .SearchedDirsCode}}- {{.}}
{{end}}
This repository is too large for GitHub to return its full tree, so SchemaBot searched only those directories. Check that the ` + "`schemabot.yaml`" + ` for this database lives under one of them and that its ` + "`database`" + ` field matches the ` + "`-d`" + ` flag value.{{else}}No ` + "`schemabot.yaml`" + ` configuration with {{.DatabaseDeclarationCode}} was found in this repository.

Check that your ` + "`schemabot.yaml`" + ` file has the correct ` + "`database`" + ` field matching the ` + "`-d`" + ` flag value.{{end}}`

const databaseNotConfiguredTemplate = "## " + glyph.Attention + ` Database Not Configured

**Database**: {{.DatabaseNameCode}}{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

This SchemaBot instance has no {{.DatabaseNameCode}} entry under ` + "`databases`" + ` in its server configuration, so it cannot plan or apply schema changes for it. A ` + "`schemabot.yaml`" + ` declaring {{.DatabaseDeclarationCode}} is not enough on its own: the database also has to be configured on the SchemaBot server.

Check that the database name, from ` + "`-d`" + ` or from ` + "`schemabot.yaml`" + `, matches one this instance serves, or ask a SchemaBot operator to configure the database.`

const databaseRepoNotAllowedTemplate = "## " + glyph.Attention + ` Database Not Available to This Repository

**Database**: {{.DatabaseNameCode}}{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

The SchemaBot server configures {{.DatabaseNameCode}} to accept schema changes from other repositories only: this repository is not in the database's ` + "`allowed_repos`" + `, so no ` + "`schemabot.yaml`" + ` in it can manage the database and none was searched.

Ask a SchemaBot operator to add this repository to the database's ` + "`allowed_repos`" + ` if it should manage the database, or check that the ` + "`-d`" + ` value names the right database.`

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
- **type** (required): {{.DatabaseTypeOptions}}`

const noConfigNoDatabaseTemplate = "## " + glyph.Info + ` No SchemaBot Configuration Found

{{with .EnvironmentHeader}}{{.}}

{{end}}{{.Attribution}}

No ` + "`schemabot.yaml`" + ` configuration file was found in this repository.

### Setup Instructions
Create a ` + "`schemabot.yaml`" + ` file in your schema directory:

` + "```yaml" + `
database: your-database-name
type: mysql
` + "```" + `

` + "`type`" + `: {{.DatabaseTypeOptions}}

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
Create a ` + "`schemabot.yaml`" + ` file in your schema directory:

{{.SetupConfigBlock}}
` + "`type`" + `: {{.DatabaseTypeOptions}}`

const configOutsideAllowedDirsTemplate = "## " + glyph.Attention + ` SchemaBot Configuration Not Authorized

**Database**: {{.DatabaseNameCode}}{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

SchemaBot found a ` + "`schemabot.yaml`" + ` configuration, but this SchemaBot instance is not configured to manage its schema directory.

**Schema directory**: {{.SchemaPathCode}}

Ask a SchemaBot operator to add this directory to {{.AllowedDirsKey}} in the server config, or move the schema config and files under an allowed directory.`

const unmanagedSchemaConfigsNoticeTemplate = "## " + glyph.Attention + ` Schema Changes Not Managed by SchemaBot

This PR changes schema under the following path(s), which this SchemaBot instance is not configured to manage:

{{range .Configs}}- {{.SchemaPath}} — declares database {{.Database}}
{{end}}
These schema changes will **not** be planned or applied, and the SchemaBot checks on this PR do not cover them.

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
	tmplDatabaseNotFound     = template.Must(template.New("databaseNotFound").Parse(databaseNotFoundTemplate))
	tmplDatabaseNotConfig    = template.Must(template.New("databaseNotConfigured").Parse(databaseNotConfiguredTemplate))
	tmplRepoTreeTruncated    = template.Must(template.New("repositoryTreeTruncated").Parse(repositoryTreeTruncatedTemplate))
	tmplDatabaseRepoDenied   = template.Must(template.New("databaseRepoNotAllowed").Parse(databaseRepoNotAllowedTemplate))
	tmplInvalidConfig        = template.Must(template.New("invalidConfig").Parse(invalidConfigTemplate))
	tmplNoConfigNoDatabase   = template.Must(template.New("noConfigNoDatabase").Parse(noConfigNoDatabaseTemplate))
	tmplNoConfigWithDatabase = template.Must(template.New("noConfigWithDatabase").Parse(noConfigWithDatabaseTemplate))
	tmplConfigNotAuthorized  = template.Must(template.New("configOutsideAllowedDirs").Parse(configOutsideAllowedDirsTemplate))
	tmplUnmanagedNotice      = template.Must(template.New("unmanagedSchemaConfigsNotice").Parse(unmanagedSchemaConfigsNoticeTemplate))
	tmplMultipleConfigs      = template.Must(template.New("multipleConfigs").Parse(multipleConfigsTemplate))
	tmplGenericError         = template.Must(template.New("genericError").Parse(genericErrorTemplate))
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
		"SchemaBot found a `schemabot.yaml` configuration, but this SchemaBot instance is not configured to manage its schema directory.",
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
// and no other deployment is expected to handle them. The database names and
// paths come from the PR's own schemabot.yaml files — untrusted input — so
// each is normalized into a safe inline code span before rendering.
func RenderUnmanagedSchemaConfigsNotice(configs []UnmanagedSchemaConfigNoticeData) string {
	normalized := make([]UnmanagedSchemaConfigNoticeData, len(configs))
	for i, cfg := range configs {
		normalized[i] = UnmanagedSchemaConfigNoticeData{
			Database:   inlineCode(cfg.Database),
			SchemaPath: inlineCode(cfg.SchemaPath),
		}
	}
	var sb strings.Builder
	data := struct {
		Configs []UnmanagedSchemaConfigNoticeData
	}{Configs: normalized}
	if err := tmplUnmanagedNotice.Execute(&sb, data); err != nil {
		return fmt.Sprintf("Error rendering template: %v", err)
	}
	return offerSupportChannel(sb.String())
}

// PreviewCommentUnmanagedSchemaConfigsNotice renders a sample notice for
// schema changes under configs this deployment does not manage, with the
// support-channel footer a configured deployment appends to it.
func PreviewCommentUnmanagedSchemaConfigsNotice() string {
	return RenderSupportChannelFooter(RenderUnmanagedSchemaConfigsNotice([]UnmanagedSchemaConfigNoticeData{
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
