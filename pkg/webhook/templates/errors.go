package templates

import (
	"fmt"
	"strings"
	"text/template"
)

// SchemaErrorData contains data for rendering schema request error comments.
type SchemaErrorData struct {
	RequestedBy        string
	Timestamp          string
	Environment        string
	DatabaseName       string
	SchemaPath         string
	CommandName        string // "plan" or "apply"
	ErrorDetail        string
	AvailableDatabases string
}

const databaseNotFoundTemplate = `## ⚠️ Database Not Found

**Database**: ` + "`{{.DatabaseName}}`" + ` | **Environment**: ` + "`{{.Environment}}`" + `

*Requested by @{{.RequestedBy}} at {{.Timestamp}} UTC*

No ` + "`schemabot.yaml`" + ` configuration with ` + "`database: {{.DatabaseName}}`" + ` was found in this repository.

Check that your ` + "`schemabot.yaml`" + ` file has the correct ` + "`database`" + ` field matching the ` + "`-d`" + ` flag value.`

const invalidConfigTemplate = `## ⚠️ No Valid SchemaBot Configuration Found

**Environment**: ` + "`{{.Environment}}`" + `

*Requested by @{{.RequestedBy}} at {{.Timestamp}} UTC*

The ` + "`schemabot.yaml`" + ` file must include ` + "`database`" + ` and ` + "`type`" + ` fields:

` + "```yaml" + `
database: your-database-name
type: mysql
` + "```" + `

- **database** (required): The database name
- **type** (required): ` + "`vitess`" + ` or ` + "`mysql`" + ``

const noConfigNoDatabaseTemplate = `## ℹ️ No SchemaBot Configuration Found

**Environment**: ` + "`{{.Environment}}`" + `

*Requested by @{{.RequestedBy}} at {{.Timestamp}} UTC*

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
schemabot {{.CommandName}} -e {{.Environment}} -d <database-name>
` + "```" + ``

const noConfigWithDatabaseTemplate = `## ℹ️ No SchemaBot Configuration Found

**Database**: ` + "`{{.DatabaseName}}`" + ` | **Environment**: ` + "`{{.Environment}}`" + `

*Requested by @{{.RequestedBy}} at {{.Timestamp}} UTC*

No ` + "`schemabot.yaml`" + ` configuration file exists in this repository.

### Setup Instructions
Create a ` + "`schemabot.yaml`" + ` file in your schema directory:

` + "```yaml" + `
database: {{.DatabaseName}}
type: mysql
` + "```" + ``

const configOutsideAllowedDirsTemplate = `## ⚠️ SchemaBot Configuration Not Authorized

**Database**: ` + "`{{.DatabaseName}}`" + ` | **Environment**: ` + "`{{.Environment}}`" + `

*Requested by @{{.RequestedBy}} at {{.Timestamp}} UTC*

SchemaBot found a ` + "`schemabot.yaml`" + ` configuration, but this SchemaBot instance is not configured to manage its schema directory.

**Schema directory**: ` + "`{{.SchemaPath}}`" + `

Ask a SchemaBot operator to add this directory to ` + "`databases.{{.DatabaseName}}.allowed_dirs`" + ` in the server config, or move the schema config and files under an allowed directory.`

const multipleConfigsTemplate = `## ⚠️ Multiple Databases Detected

**Environment**: ` + "`{{.Environment}}`" + `

*Requested by @{{.RequestedBy}} at {{.Timestamp}} UTC*

This repository has multiple ` + "`schemabot.yaml`" + ` configurations.

### Available Databases

{{.AvailableDatabases}}

### How to specify a database

Use the ` + "`-d`" + ` flag:

` + "```" + `
schemabot {{.CommandName}} -e {{.Environment}} -d <database-name>
` + "```" + ``

const genericErrorTemplate = `## ❌ {{.CommandName}} Failed

**Environment**: ` + "`{{.Environment}}`" + `

*Requested by @{{.RequestedBy}} at {{.Timestamp}} UTC*

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
