package templates

import (
	"fmt"
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
	whoCanApply := "🔐 **Who can apply**: only these teams and users can run `schemabot apply` for `orders`, once the PR has any approval it needs. Approving the PR does not let anyone else run it, so if you are not listed, ask one of them to run it.\n" +
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

	// Auto-plan on a deployment with one environment builds the comment as a
	// multi-environment one that renders as that environment's plan; the list
	// set on the wrapper alone still reaches its footer.
	t.Run("one environment through the multi-environment comment", func(t *testing.T) {
		child := plan("staging")
		child.ApplyAuthorization = nil
		out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
			Database: "orders", DatabaseType: "mysql", IsMySQL: true,
			Environments:       []string{"staging"},
			Plans:              map[string]*PlanCommentData{"staging": child},
			ApplyAuthorization: auth,
		})
		assert.Contains(t, out, "```\nschemabot apply -e staging\n```\n\n"+whoCanApply, out)
	})

	// A multi-environment comment whose footer offers no apply, here because
	// one environment failed to plan and the other has no changes, names
	// nobody.
	t.Run("no environment to apply names nobody", func(t *testing.T) {
		staging := plan("staging")
		staging.Changes = nil
		out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
			Database: "orders", DatabaseType: "mysql", IsMySQL: true,
			Environments:       []string{"staging", "production"},
			Plans:              map[string]*PlanCommentData{"staging": staging},
			Errors:             map[string]string{"production": "plan failed; see server logs"},
			ApplyAuthorization: auth,
		})
		assert.Contains(t, out, "**Production** failed to plan.", out)
		assert.NotContains(t, out, "🔐", out)
	})

	t.Run("a long list names ten and counts the rest", func(t *testing.T) {
		data := *plan("staging")
		var users []string
		for i := range 12 {
			users = append(users, fmt.Sprintf("user%02d", i))
		}
		data.ApplyAuthorization = &ApplyAuthorizationData{OperatorPrincipals: users}
		out := RenderPlanComment(data)
		assert.Contains(t, out, "- Operators: `user00`, ", out)
		assert.Contains(t, out, ", `user09`, and 2 more\n", out)
		assert.NotContains(t, out, "`user10`", out)
	})

	t.Run("only admins", func(t *testing.T) {
		data := *plan("staging")
		data.ApplyAuthorization = &ApplyAuthorizationData{OtherPrincipals: []string{"acme/db-admins"}}
		out := RenderPlanComment(data)
		assert.Contains(t, out, "ask one of them to run it.\n- Admins: `acme/db-admins`\n", out)
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
