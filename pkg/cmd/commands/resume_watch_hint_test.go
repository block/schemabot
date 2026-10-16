package commands

import (
	"strings"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/cliname"
)

// recordRoute records g's routing flags the way main does after parsing, and
// clears them when the test ends so later tests render unrouted hints.
func recordRoute(t *testing.T, g Globals) {
	t.Helper()
	cliname.SetRoute(g.RouteFlags()...)
	t.Cleanup(func() { cliname.SetRoute() })
}

// An operator interrupts a log watch of a production apply started with an
// explicit profile and endpoint. The resume hint names the environment and
// both routing flags, so pasting it reaches the same control plane rather
// than one a wrapper picks for an omitted environment. A watch started with
// no routing flags keeps the short form.
func TestResumeWatchHint_CarriesRouting(t *testing.T) {
	t.Run("default invocation stays short", func(t *testing.T) {
		recordRoute(t, Globals{})
		assert.Equal(t, "watch it again with 'schemabot progress apply-7c2e'", resumeWatchHint("apply-7c2e", ""))
	})

	t.Run("production watch names the environment", func(t *testing.T) {
		recordRoute(t, Globals{})
		assert.Equal(t, "watch it again with 'schemabot progress apply-7c2e -e production'", resumeWatchHint("apply-7c2e", "production"))
	})

	t.Run("profile and endpoint are carried", func(t *testing.T) {
		recordRoute(t, Globals{Profile: "prod", Endpoint: "https://schemabot.example/api"})
		assert.Equal(t,
			"watch it again with 'schemabot progress apply-7c2e -e production --profile prod --endpoint https://schemabot.example/api'",
			resumeWatchHint("apply-7c2e", "production"))
	})

	t.Run("stopped notice carries the same hint", func(t *testing.T) {
		recordRoute(t, Globals{Profile: "prod"})
		assert.Equal(t,
			"Stopped watching apply apply-7c2e; stopping the watch does not affect the apply; "+
				"watch it again with 'schemabot progress apply-7c2e -e production --profile prod'",
			progressStoppedMessage("apply-7c2e", "production"))
	})
}

// The routing flags are printed, so they never carry the token, and the
// endpoint loses any credentials, query, or fragment it was given.
func TestGlobalsRouteFlags_NeverPrintsSecrets(t *testing.T) {
	g := Globals{
		Profile:  "prod",
		Endpoint: "https://ops:hunter2@schemabot.example/api?token=abc123#frag",
		Token:    "bearer-secret",
	}

	flags := g.RouteFlags()

	assert.Equal(t, []string{"--profile", "prod", "--endpoint", "https://schemabot.example/api"}, flags)
	joined := strings.Join(flags, " ")
	for _, secret := range []string{"hunter2", "ops:", "abc123", "bearer-secret", "frag"} {
		assert.NotContains(t, joined, secret)
	}
	assert.Empty(t, (&Globals{Token: "bearer-secret"}).RouteFlags(), "a token alone selects no route")
}

// The hint is only right if pasting it selects what the original command
// selected. Parse the rendered command back through kong and compare.
func TestResumeWatchHint_ParsesBackToTheSameRoute(t *testing.T) {
	recordRoute(t, Globals{Profile: "prod", Endpoint: "https://schemabot.example"})
	hint := pastedResumeCommand("apply-7c2e", "production")

	args := strings.Fields(strings.TrimPrefix(hint, cliname.Name()+" "))
	var cli struct {
		Globals
		Progress ProgressCmd `cmd:""`
	}
	parser, err := kong.New(&cli, kong.Name("schemabot"))
	require.NoError(t, err)
	_, err = parser.Parse(args)
	require.NoError(t, err, "the hint %q must parse", hint)

	assert.Equal(t, "apply-7c2e", cli.Progress.ApplyID)
	assert.Equal(t, "production", cli.Progress.Environment)
	assert.Equal(t, "prod", cli.Profile)
	assert.Equal(t, "https://schemabot.example", cli.Endpoint)
}

// pastedResumeCommand extracts the command from the resume hint,
// so the round-trip test checks the text the operator actually copies.
func pastedResumeCommand(applyID, environment string) string {
	hint := resumeWatchHint(applyID, environment)
	return strings.TrimSuffix(strings.TrimPrefix(hint, "watch it again with '"), "'")
}

// An operator detaches from the interactive watch of a production apply. The
// reattach line names the environment, matching the stop line beside it.
func TestDetachedView_ReattachNamesEnvironment(t *testing.T) {
	recordRoute(t, Globals{Profile: "prod"})
	model := NewWatchModel("http://schemabot.test", "orders", "production", true)
	model.applyID = "apply-7c2e"

	view := model.detachedView()

	assert.Contains(t, view, "To reattach: schemabot progress apply-7c2e -e production --profile prod\n")
	assert.Contains(t, view, "To stop:     schemabot stop -e production apply-7c2e --profile prod\n")
}
