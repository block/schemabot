package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
		assert.Contains(t, body, "- `services/inventory schema` — declares database `` inven`tory ``")
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
		assert.Contains(t, body, "**Available environments**: `` pro`duction ``, `sta ging`")
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

func TestSetupGuidanceExperimentalStrata(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, database := range []string{"", "example"} {
			data := SchemaErrorData{ExperimentalStrataEnabled: enabled, DatabaseName: database}
			for _, body := range []string{RenderNoConfig(data), RenderInvalidConfig(data)} {
				assert.Contains(t, body, "`mysql`")
				assert.Contains(t, body, "`postgres`")
				assert.Contains(t, body, "`vitess`")
				if enabled {
					assert.Contains(t, body, "`strata` (experimental)")
				} else {
					assert.NotContains(t, body, "strata")
				}
			}
		}
	}
}

func TestInvalidConfigDatabaseTypeLine(t *testing.T) {
	for _, tc := range []struct {
		enabled bool
		want    string
	}{
		{false, "- **type** (required): `mysql`, `postgres`, or `vitess`"},
		{true, "- **type** (required): `mysql`, `postgres`, `vitess`, or `strata` (experimental)"},
	} {
		body := RenderInvalidConfig(SchemaErrorData{ExperimentalStrataEnabled: tc.enabled})
		var got string
		for line := range strings.SplitSeq(body, "\n") {
			if strings.HasPrefix(line, "- **type**") {
				got = line
			}
		}
		assert.Equal(t, tc.want, got)
	}
}

// TestRenderDatabaseNotFoundScopedSearch pins the wording for a repository too
// large to search in full: the comment lists the configured directories that
// were probed instead of claiming the whole repository was searched, and the
// unscoped rendering keeps the repository-wide claim.
func TestRenderDatabaseNotFoundScopedSearch(t *testing.T) {
	t.Run("scoped search names the directories", func(t *testing.T) {
		body := RenderDatabaseNotFound(SchemaErrorData{
			Timestamp:    "2026-07-16 18:56:00",
			Environment:  "staging",
			DatabaseName: "payments",
			SearchedDirs: []string{"services/payments/schema", "services/payments/legacy-schema"},
		})
		assert.Contains(t, body, "was found in the schema directories configured for this database on the SchemaBot server:")
		assert.Contains(t, body, "- `services/payments/schema`\n- `services/payments/legacy-schema`\n")
		assert.Contains(t, body, "SchemaBot searched only those directories")
		assert.NotContains(t, body, "was found in this repository")
	})

	t.Run("full search keeps the repository-wide claim", func(t *testing.T) {
		body := RenderDatabaseNotFound(SchemaErrorData{
			Timestamp:    "2026-07-16 18:56:00",
			Environment:  "staging",
			DatabaseName: "payments",
		})
		assert.Contains(t, body, "was found in this repository")
		assert.NotContains(t, body, "configured for this database")
	})
}

// TestRenderDatabaseNotConfigured pins the comment for a database the server
// has no configuration for: it names the database, says the repository's
// schemabot.yaml may be correct, and points at the server-side remedy.
func TestRenderDatabaseNotConfigured(t *testing.T) {
	body := RenderDatabaseNotConfigured(SchemaErrorData{
		RequestedBy:  "hubot",
		Timestamp:    "2026-07-16 18:56:00",
		Environment:  "staging",
		DatabaseName: "payments",
		CommandName:  "apply",
	})
	assert.Contains(t, body, "## ⚠️ Database Not Configured")
	assert.Contains(t, body, "**Database**: `payments` | **Environment**: `staging`")
	assert.Contains(t, body, "*Requested by @hubot at 2026-07-16 18:56:00 UTC*")
	assert.Contains(t, body, "has no `payments` entry under `databases` in its server configuration")
	assert.Contains(t, body, "A `schemabot.yaml` declaring `database: payments` is not enough on its own")
	assert.Contains(t, body, "Check that the database name, from `-d` or from `schemabot.yaml`, matches one this instance serves, or ask a SchemaBot operator to configure the database")
	assert.NotContains(t, body, "was found in this repository")
}

// TestRenderRepositoryTreeTruncated pins the comment for a repository GitHub
// could not return in full: a database-scoped command names the database and
// the two server-side causes, an unscoped command explains the repo-wide
// search instead, and the header follows the same database/environment
// rules as the other schema request errors.
func TestRenderRepositoryTreeTruncated(t *testing.T) {
	t.Run("database-scoped command", func(t *testing.T) {
		body := RenderRepositoryTreeTruncated(SchemaErrorData{
			RequestedBy:  "hubot",
			Timestamp:    "2026-07-16 18:56:00",
			Environment:  "staging",
			DatabaseName: "payments",
			CommandName:  "apply",
		})
		assert.Contains(t, body, "## ⚠️ Repository Too Large to Search")
		assert.Contains(t, body, "**Database**: `payments` | **Environment**: `staging`")
		assert.Contains(t, body, "GitHub returned a truncated repository tree")
		assert.Contains(t, body, "it has none it can search exhaustively for `payments`: the database is not configured on this instance, or its `allowed_dirs` leave the location of its config open")
		assert.Contains(t, body, "or check that the `-d` value names a database this instance serves")
	})

	t.Run("unscoped auto-plan", func(t *testing.T) {
		body := RenderRepositoryTreeTruncated(SchemaErrorData{
			Timestamp:    "2026-07-16 18:56:00",
			Environments: []string{"staging"},
			CommandName:  "plan",
		})
		assert.Contains(t, body, "## ⚠️ Repository Too Large to Search\n\n**Environment**: `staging`\n\n*Triggered automatically by a pull request update at 2026-07-16 18:56:00 UTC*")
		assert.Contains(t, body, "for this repository's databases, and those do not bound where every config may live")
		assert.Contains(t, body, "give each of this repository's databases an `allowed_dirs` entry")
		assert.NotContains(t, body, "**Database**")
		assert.NotContains(t, body, "`-d`")
	})

	t.Run("unscoped deployment drops the header segment", func(t *testing.T) {
		body := RenderRepositoryTreeTruncated(SchemaErrorData{
			RequestedBy: "hubot",
			Timestamp:   "2026-07-16 18:56:00",
			CommandName: "plan",
		})
		assert.Contains(t, body, "## ⚠️ Repository Too Large to Search\n\n*Requested by @hubot at 2026-07-16 18:56:00 UTC*")
	})
}
