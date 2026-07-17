package templates

import (
	"fmt"
	"strings"
	"text/template"
)

// SchemaErrorData contains data for rendering schema request error comments.
type SchemaErrorData struct {
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
}

// EnvironmentHeader renders the environment header segment: the single
// environment the command targeted, or the deployment's environment scope
// when the command spanned environments (multi-environment plans, including
// auto-plans). Returns "" when neither is known so templates drop the
// segment — never an empty code span or a vague placeholder.
func (d SchemaErrorData) EnvironmentHeader() string {
	if d.Environment != "" {
		return "**Environment**: " + markdownInlineCode(d.Environment)
	}
	switch len(d.Environments) {
	case 0:
		return ""
	case 1:
		return "**Environment**: " + markdownInlineCode(d.Environments[0])
	default:
		quoted := make([]string, len(d.Environments))
		for i, name := range d.Environments {
			quoted[i] = markdownInlineCode(name)
		}
		return "**Environments**: " + strings.Join(quoted, ", ")
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

const databaseNotFoundTemplate = `## ⚠️ Database Not Found

**Database**: ` + "`{{.DatabaseName}}`" + `{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

No ` + "`schemabot.yaml`" + ` configuration with ` + "`database: {{.DatabaseName}}`" + ` was found in this repository.

Check that your ` + "`schemabot.yaml`" + ` file has the correct ` + "`database`" + ` field matching the ` + "`-d`" + ` flag value.`

const invalidConfigTemplate = `## ⚠️ No Valid SchemaBot Configuration Found

{{with .EnvironmentHeader}}{{.}}

{{end}}{{.Attribution}}

The ` + "`schemabot.yaml`" + ` file must include ` + "`database`" + ` and ` + "`type`" + ` fields:

` + "```yaml" + `
database: your-database-name
type: mysql
` + "```" + `

- **database** (required): The database name
- **type** (required): ` + "`vitess`" + ` or ` + "`mysql`" + ``

const noConfigNoDatabaseTemplate = `## ℹ️ No SchemaBot Configuration Found

{{with .EnvironmentHeader}}{{.}}

{{end}}{{.Attribution}}

No ` + "`schemabot.yaml`" + ` configuration file was found in this repository.

### Setup Instructions
Create a ` + "`schemabot.yaml`" + ` file in your schema directory:

` + "```yaml" + `
database: your-database-name
type: mysql
` + "```" + `

### If you already have a config
Use the ` + "`-d`" + ` flag to specify which database to {{.CommandName}}:

` + "```" + `
schemabot {{.CommandName}} -e {{.ExampleEnvironment}} -d <database-name>
` + "```" + ``

const noConfigWithDatabaseTemplate = `## ℹ️ No SchemaBot Configuration Found

**Database**: ` + "`{{.DatabaseName}}`" + `{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

No ` + "`schemabot.yaml`" + ` configuration file exists in this repository.

### Setup Instructions
Create a ` + "`schemabot.yaml`" + ` file in your schema directory:

` + "```yaml" + `
database: {{.DatabaseName}}
type: mysql
` + "```" + ``

const configOutsideAllowedDirsTemplate = `## ⚠️ SchemaBot Configuration Not Authorized

**Database**: ` + "`{{.DatabaseName}}`" + `{{with .EnvironmentHeader}} | {{.}}{{end}}

{{.Attribution}}

SchemaBot found a ` + "`schemabot.yaml`" + ` configuration, but this SchemaBot instance is not configured to manage its schema directory.

**Schema directory**: ` + "`{{.SchemaPath}}`" + `

Ask a SchemaBot operator to add this directory to ` + "`databases.{{.DatabaseName}}.allowed_dirs`" + ` in the server config, or move the schema config and files under an allowed directory.`

const multipleConfigsTemplate = `## ⚠️ Multiple Databases Detected

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

const genericErrorTemplate = `## ❌ {{.CommandName}} Failed

{{with .EnvironmentHeader}}{{.}}

{{end}}{{.Attribution}}

### Error

> {{.ErrorDetail}}`

// Compiled templates.
var (
	tmplDatabaseNotFound     = template.Must(template.New("databaseNotFound").Parse(databaseNotFoundTemplate))
	tmplInvalidConfig        = template.Must(template.New("invalidConfig").Parse(invalidConfigTemplate))
	tmplNoConfigNoDatabase   = template.Must(template.New("noConfigNoDatabase").Parse(noConfigNoDatabaseTemplate))
	tmplNoConfigWithDatabase = template.Must(template.New("noConfigWithDatabase").Parse(noConfigWithDatabaseTemplate))
	tmplConfigNotAuthorized  = template.Must(template.New("configOutsideAllowedDirs").Parse(configOutsideAllowedDirsTemplate))
	tmplMultipleConfigs      = template.Must(template.New("multipleConfigs").Parse(multipleConfigsTemplate))
	tmplGenericError         = template.Must(template.New("genericError").Parse(genericErrorTemplate))
)

// RenderDatabaseNotFound renders the "database not found" error comment.
func RenderDatabaseNotFound(data SchemaErrorData) string {
	return renderTemplate(tmplDatabaseNotFound, data)
}

// RenderInvalidConfig renders the "invalid config" error comment.
func RenderInvalidConfig(data SchemaErrorData) string {
	return renderTemplate(tmplInvalidConfig, data)
}

// RenderNoConfig renders the "no config found" error comment.
func RenderNoConfig(data SchemaErrorData) string {
	if data.DatabaseName == "" {
		return renderTemplate(tmplNoConfigNoDatabase, data)
	}
	return renderTemplate(tmplNoConfigWithDatabase, data)
}

// RenderConfigNotAuthorized renders the error shown when schemabot.yaml exists
// but its schema directory is outside the server-side allowed_dirs boundary.
func RenderConfigNotAuthorized(data SchemaErrorData) string {
	return renderTemplate(tmplConfigNotAuthorized, data)
}

// RenderMultipleConfigs renders the "multiple configs" error comment.
func RenderMultipleConfigs(data SchemaErrorData) string {
	return renderTemplate(tmplMultipleConfigs, data)
}

// RenderGenericError renders a generic error comment.
func RenderGenericError(data SchemaErrorData) string {
	// Capitalize command name for header
	data.CommandName = capitalizeFirst(data.CommandName)
	return renderTemplate(tmplGenericError, data)
}

// RenderInvalidCommand generates an error message for unrecognized commands.
func RenderInvalidCommand() string {
	return "## ❌ Invalid Command\n\nThat command wasn't recognized. Available commands:\n\n" + commandReference()
}

// RenderInvalidEnv generates an error message when the -e value does not name
// a configured environment — whether malformed (e.g. a flag glued onto the
// value by a missing space) or simply not an environment any instance
// handles. The configured environment names are normalized for markdown
// display so an unexpected character cannot break the comment.
func RenderInvalidEnv(action string, available []string) string {
	quoted := make([]string, len(available))
	for i, name := range available {
		quoted[i] = markdownInlineCode(name)
	}
	availableLine := ""
	if len(quoted) > 0 {
		availableLine = "\n**Available environments**: " + strings.Join(quoted, ", ") + "\n"
	}
	return fmt.Sprintf(`## ❌ Invalid Environment

`+"`-e`"+` must name one of the configured environments.
%s
**Usage**: `+"`schemabot %s -e <environment> [flags]`", availableLine, action)
}

// markdownInlineCode renders a value as a markdown inline code span,
// normalizing characters that would break the span: backticks are stripped
// and whitespace (including newlines) collapses to single spaces.
func markdownInlineCode(s string) string {
	s = strings.ReplaceAll(s, "`", "")
	return "`" + strings.Join(strings.Fields(s), " ") + "`"
}

// RenderMissingEnv generates an error message when -e flag is missing.
func RenderMissingEnv(action string) string {
	return fmt.Sprintf(`## ❌ Missing Argument

You'll need to specify which environment to target with the `+"`-e`"+` flag.

**Usage**: `+"`schemabot %s -e <environment>`"+`

**Example**:
`+"```"+`
schemabot %s -e staging
`+"```", action, action)
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
