package templates

import (
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
