package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// With PR command authorization on, the plan comment names who can run its
// apply right under the apply command, so a PR author who is not listed asks
// an operator to review and apply in one pass instead of finding out from a
// refused apply. Principals render as inline code, never as @-mentions, so a
// plan posted on every push does not notify every listed team.
func TestPlanCommentListsWhoCanApply(t *testing.T) {
	auth := &ApplyAuthorizationData{
		OperatorPrincipals: []string{"acme/orders-operators", "lee"},
		OtherPrincipals:    []string{"acme/db-admins"},
	}
	plan := func(env string) *PlanCommentData {
		return &PlanCommentData{
			Database: "orders", Environment: env, DatabaseType: "mysql", IsMySQL: true,
			Changes:            []KeyspaceChangeData{{Keyspace: "orders", Statements: []string{"ALTER TABLE `orders` ADD COLUMN `note` varchar(255);"}}},
			ApplyAuthorization: auth,
		}
	}
	whoCanApply := "🔐 **Who can apply**: only these teams and users can run `schemabot apply` for `orders`. Approving the PR does not let anyone else run it, so if you are not listed, ask one of them to review the PR and run the command.\n" +
		"- Operators: `acme/orders-operators`, `lee`\n" +
		"- Admins: `acme/db-admins`\n"

	t.Run("single environment", func(t *testing.T) {
		out := RenderPlanComment(*plan("staging"))
		assert.Contains(t, out, "```\nschemabot apply -e staging\n```\n\n"+whoCanApply, out)
		assert.NotContains(t, out, "@acme", out)
	})

	t.Run("every environment lists it once", func(t *testing.T) {
		out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
			Database: "orders", DatabaseType: "mysql", IsMySQL: true,
			Environments:       []string{"staging", "production"},
			Plans:              map[string]*PlanCommentData{"staging": plan("staging"), "production": plan("production")},
			ApplyAuthorization: auth,
		})
		assert.Equal(t, 1, strings.Count(out, "**Who can apply**"), out)
		assert.Contains(t, out, whoCanApply, out)
	})

	t.Run("only admins", func(t *testing.T) {
		data := *plan("staging")
		data.ApplyAuthorization = &ApplyAuthorizationData{OtherPrincipals: []string{"acme/db-admins"}}
		out := RenderPlanComment(data)
		assert.Contains(t, out, "the command.\n- Admins: `acme/db-admins`\n", out)
		assert.NotContains(t, out, "- Operators:", out)
	})

	t.Run("nobody configured", func(t *testing.T) {
		data := *plan("staging")
		data.ApplyAuthorization = &ApplyAuthorizationData{}
		out := RenderPlanComment(data)
		assert.Contains(t, out, "🔐 No team or user can run `schemabot apply` for `orders` yet. Ask a SchemaBot admin to configure its operators.\n", out)
	})

	t.Run("authorization off", func(t *testing.T) {
		data := *plan("staging")
		data.ApplyAuthorization = nil
		assert.NotContains(t, RenderPlanComment(data), "🔐", RenderPlanComment(data))
	})

	// A comment that offers no apply has nobody to name.
	t.Run("no apply offered", func(t *testing.T) {
		data := *plan("staging")
		data.Changes = nil
		assert.NotContains(t, RenderPlanComment(data), "🔐", RenderPlanComment(data))

		locked := *plan("staging")
		locked.IsLocked = true
		locked.PendingManualConfirmation = true
		assert.NotContains(t, RenderPlanComment(locked), "🔐", RenderPlanComment(locked))
	})
}
