package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
)

// TestRenderGenericErrorAutoPlan covers the failure comment for a
// system-triggered auto-plan: there is no requesting user and no single
// target environment, so the comment must attribute the plan to the pull
// request update and show the deployment's concrete environment scope — in a
// multi-deployment topology each deployment handles its own environments, so
// naming them is what tells the reader which deployment failed. It must
// never render an empty code span or a bare @ mention.
func TestRenderGenericErrorAutoPlan(t *testing.T) {
	t.Run("deployment scoped to one environment names it", func(t *testing.T) {
		body := RenderGenericError(SchemaErrorData{
			Timestamp:    "2026-07-16 18:56:00",
			Environments: []string{"staging"},
			CommandName:  "plan",
			ErrorDetail:  "failed to fetch repository contents",
		})

		assert.Contains(t, body, "## ❌ Plan Failed")
		assert.Contains(t, body, "**Environment**: `staging`")
		assert.Contains(t, body, "*Triggered automatically by a pull request update at 2026-07-16 18:56:00 UTC*")
		assert.Contains(t, body, "> failed to fetch repository contents")
		assert.NotContains(t, body, "**Environment**: ``")
		assert.NotContains(t, body, "Requested by")
	})

	t.Run("deployment scoped to several environments lists them", func(t *testing.T) {
		body := RenderGenericError(SchemaErrorData{
			Timestamp:    "2026-07-16 18:56:00",
			Environments: []string{"staging", "production"},
			CommandName:  "plan",
			ErrorDetail:  "failed to fetch repository contents",
		})

		assert.Contains(t, body, "**Environments**: `staging`, `production`")
	})

	t.Run("unscoped deployment omits the environment header", func(t *testing.T) {
		body := RenderGenericError(SchemaErrorData{
			Timestamp:   "2026-07-16 18:56:00",
			CommandName: "plan",
			ErrorDetail: "failed to fetch repository contents",
		})

		assert.Contains(t, body, "## ❌ Plan Failed")
		assert.NotContains(t, body, "**Environment")
		assert.Contains(t, body, "*Triggered automatically by a pull request update at 2026-07-16 18:56:00 UTC*")
	})
}

// TestRenderDatabaseNotFoundHeader pins the joined Database | Environment
// header: the separator only appears when there is an environment segment to
// join, so an unscoped deployment renders a clean database-only header.
func TestRenderDatabaseNotFoundHeader(t *testing.T) {
	t.Run("deployment scope joins the header", func(t *testing.T) {
		body := RenderDatabaseNotFound(SchemaErrorData{
			Timestamp:    "2026-07-16 18:56:00",
			Environments: []string{"staging"},
			DatabaseName: "testapp",
		})
		assert.Contains(t, body, "**Database**: `testapp` | **Environment**: `staging`")
	})

	t.Run("unscoped deployment drops the separator", func(t *testing.T) {
		body := RenderDatabaseNotFound(SchemaErrorData{
			Timestamp:    "2026-07-16 18:56:00",
			DatabaseName: "testapp",
		})
		assert.Contains(t, body, "**Database**: `testapp`\n")
		assert.NotContains(t, body, " | ")
		assert.NotContains(t, body, "**Environment")
	})
}

// TestRenderGenericErrorUserRequested pins the user-issued rendering: a
// single environment shows as a code span and the footer names the requester.
func TestRenderGenericErrorUserRequested(t *testing.T) {
	body := RenderGenericError(SchemaErrorData{
		RequestedBy: "octocat",
		Timestamp:   "2026-07-16 18:56:00",
		Environment: "staging",
		CommandName: "plan",
		ErrorDetail: "boom",
	})

	assert.Contains(t, body, "**Environment**: `staging`")
	assert.Contains(t, body, "*Requested by @octocat at 2026-07-16 18:56:00 UTC*")
	assert.NotContains(t, body, "Triggered automatically")
}

// TestRenderNoConfigUsageExample verifies the pasteable usage example: the
// requested environment when one was given, the deployment's sole
// environment when it is scoped to exactly one, otherwise a placeholder —
// never an empty -e value.
func TestRenderNoConfigUsageExample(t *testing.T) {
	t.Run("unscoped multi-environment command uses a placeholder", func(t *testing.T) {
		body := RenderNoConfig(SchemaErrorData{
			Timestamp:   "2026-07-16 18:56:00",
			CommandName: "plan",
		})
		assert.Contains(t, body, "schemabot plan -e <environment> -d <database-name>")
		assert.NotContains(t, body, "**Environment")
	})

	t.Run("single-scope deployment uses its environment", func(t *testing.T) {
		body := RenderNoConfig(SchemaErrorData{
			Timestamp:    "2026-07-16 18:56:00",
			Environments: []string{"staging"},
			CommandName:  "plan",
		})
		assert.Contains(t, body, "schemabot plan -e staging -d <database-name>")
		assert.Contains(t, body, "**Environment**: `staging`")
	})

	t.Run("multi-scope deployment keeps the placeholder", func(t *testing.T) {
		body := RenderNoConfig(SchemaErrorData{
			Timestamp:    "2026-07-16 18:56:00",
			Environments: []string{"staging", "production"},
			CommandName:  "plan",
		})
		assert.Contains(t, body, "schemabot plan -e <environment> -d <database-name>")
		assert.Contains(t, body, "**Environments**: `staging`, `production`")
	})

	t.Run("single-environment command uses the environment", func(t *testing.T) {
		body := RenderNoConfig(SchemaErrorData{
			RequestedBy: "octocat",
			Timestamp:   "2026-07-16 18:56:00",
			Environment: "staging",
			CommandName: "plan",
		})
		assert.Contains(t, body, "schemabot plan -e staging -d <database-name>")
	})
}

// TestRenderMultipleConfigsUsageExample verifies the multi-database picker
// keeps a pasteable -e value and the requester attribution when a user issues
// a command without scoping it to one environment.
func TestRenderMultipleConfigsUsageExample(t *testing.T) {
	body := RenderMultipleConfigs(SchemaErrorData{
		RequestedBy:        "octocat",
		Timestamp:          "2026-07-16 18:56:00",
		CommandName:        "plan",
		AvailableDatabases: "- `testapp`\n- `payments`",
	})
	assert.Contains(t, body, "schemabot plan -e <environment> -d <database-name>")
	assert.Contains(t, body, "*Requested by @octocat at 2026-07-16 18:56:00 UTC*")
}

func TestRenderUnmanagedSchemaConfigsNotice(t *testing.T) {
	t.Run("lists each dropped config with its database", func(t *testing.T) {
		body := RenderUnmanagedSchemaConfigsNotice([]UnmanagedSchemaConfigNoticeData{
			{Database: "inventory", SchemaPath: "services/inventory/schema"},
			{Database: "billing", SchemaPath: "services/billing/schema"},
		})
		assert.Contains(t, body, "## ⚠️ Schema Changes Not Managed by SchemaBot")
		assert.Contains(t, body, "- `services/inventory/schema` — declares database `inventory`")
		assert.Contains(t, body, "- `services/billing/schema` — declares database `billing`")
		assert.Contains(t, body, "will **not** be planned or applied")
		assert.Contains(t, body, "`allowed_dirs`")
	})

	t.Run("normalizes values that would break markdown code spans", func(t *testing.T) {
		body := RenderUnmanagedSchemaConfigsNotice([]UnmanagedSchemaConfigNoticeData{
			{Database: "inven`tory", SchemaPath: "services/inventory\nschema"},
		})
		assert.Contains(t, body, "- `services/inventory schema` — declares database `inventory`")
		assert.NotContains(t, body, "``")
		assert.NotContains(t, body, "inventory\nschema")
	})
}

func TestRenderInvalidEnv(t *testing.T) {
	t.Run("lists the configured environments", func(t *testing.T) {
		body := RenderInvalidEnv("apply", []string{"production", "staging"})
		assert.Contains(t, body, "Invalid Environment")
		assert.Contains(t, body, "must name one of the configured environments")
		assert.Contains(t, body, "**Available environments**: `production`, `staging`")
		assert.Contains(t, body, "`schemabot apply -e <environment> [flags]`")
	})

	t.Run("omits the available line when no environments are configured", func(t *testing.T) {
		body := RenderInvalidEnv("apply", nil)
		assert.Contains(t, body, "Invalid Environment")
		assert.NotContains(t, body, "Available environments")
	})

	t.Run("normalizes names that would break markdown code spans", func(t *testing.T) {
		body := RenderInvalidEnv("apply", []string{"pro`duction", "sta\nging"})
		assert.Contains(t, body, "`production`")
		assert.Contains(t, body, "`sta ging`")
		assert.NotContains(t, body, "``")
	})
}

// The generic command-failure comment renders untrusted engine and
// infrastructure error text: internal endpoints are redacted, HTML markup is
// escaped so it renders as text, and a multi-line error stays inside the
// blockquote instead of escaping into comment markup.
func TestRenderGenericErrorSanitizesDetail(t *testing.T) {
	body := RenderGenericError(SchemaErrorData{
		Timestamp:   "2026-07-16 18:56:00",
		CommandName: "plan",
		ErrorDetail: "dial tcp db-primary.internal:3306: connection refused\n# not a heading",
	})

	assert.NotContains(t, body, "db-primary.internal", "internal endpoints are redacted")
	assert.Contains(t, body, "> dial tcp [endpoint redacted]: connection refused\n> # not a heading",
		"a multi-line error stays inside the blockquote")

	body = RenderGenericError(SchemaErrorData{
		Timestamp:   "2026-07-16 18:56:00",
		CommandName: "plan",
		ErrorDetail: "unexpected <img src=x> in output",
	})
	assert.Contains(t, body, "&lt;img src=x&gt;", "HTML markup is escaped")
	assert.NotContains(t, body, "<img", "raw markup never reaches the comment")
}

// A user who has not got a working schemabot.yaml yet reaches these comments
// first, so each one must name every database type the server accepts and
// carry a link to the page that documents the file. Offering a narrower set of
// types than config validation takes sends a reader to "fix" a config that was
// already correct.
func TestFirstContactConfigCommentsTeachTheFile(t *testing.T) {
	data := SchemaErrorData{
		Timestamp:   "2026-07-16 18:56:00",
		RequestedBy: "octocat",
		Environment: "staging",
		CommandName: "plan",
	}

	comments := map[string]string{
		"invalid config":     RenderInvalidConfig(data),
		"no config":          RenderNoConfig(data),
		"no config with -d":  RenderNoConfig(withDatabase(data, "inventory")),
		"database not found": RenderDatabaseNotFound(withDatabase(data, "inventory")),
	}

	for name, body := range comments {
		t.Run(name, func(t *testing.T) {
			assert.Contains(t, body, ui.SchemaConfigDocURL, "the comment links the schemabot.yaml docs")
		})
	}

	for _, dbType := range storage.DatabaseTypes() {
		t.Run("offers type "+dbType, func(t *testing.T) {
			assert.Contains(t, RenderInvalidConfig(data), "`"+dbType+"`")
			assert.Contains(t, RenderNoConfig(data), "`"+dbType+"`")
			assert.Contains(t, RenderNoConfig(withDatabase(data, "inventory")), "`"+dbType+"`")
		})
	}
}

func withDatabase(data SchemaErrorData, database string) SchemaErrorData {
	data.DatabaseName = database
	return data
}
