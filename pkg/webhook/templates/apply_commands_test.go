package templates

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The rejection lists the teams and users allowed to run the command, so a
// blocked user knows who to ask instead of guessing at the access model. The
// principals render as inline code, never @-mentions — the list is guidance,
// and mentions would notify every admin and operator on every rejection.
func TestRenderPRCommandNotAuthorizedListsPrincipals(t *testing.T) {
	out := RenderPRCommandNotAuthorized(ActorAuthorizationCommentData{
		RequestedBy: "dave", CommandName: "apply", Database: "orders", Environment: "production",
		AuthorizedPrincipals: []string{"octocat/db-admins", "octocat/orders-operators", "kara"},
	})
	require.Contains(t, out, "@dave is not authorized")
	require.Contains(t, out, "**Who can run this command**")
	require.Contains(t, out, "- `octocat/db-admins`")
	require.Contains(t, out, "- `octocat/orders-operators`")
	require.Contains(t, out, "- `kara`")
	require.NotContains(t, out, "@octocat/db-admins", "team principals must never render as mentions")
	require.NotContains(t, out, "@kara", "user principals must never render as mentions")

	fallback := RenderPRCommandNotAuthorized(ActorAuthorizationCommentData{
		RequestedBy: "dave", CommandName: "apply", Database: "orders", Environment: "production",
	})
	require.Contains(t, fallback, "A configured SchemaBot admin/database operator must run this command.")
	require.NotContains(t, fallback, "Who can run this command")
}
