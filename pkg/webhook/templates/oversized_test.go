package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/ui"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A body GitHub accepts passes through untouched, right up to the limit that
// leaves the poster's footer its headroom.
func TestFitGitHubCommentKeepsABodyThatFits(t *testing.T) {
	body := "## Schema Change Apply — Production\n\n" + strings.Repeat("x", commentBodyLimit-40)
	require.LessOrEqual(t, len(body), commentBodyLimit)

	fitted, replaced := FitGitHubComment(body, "", "production")

	assert.False(t, replaced)
	assert.Equal(t, body, fitted)
}

// A body over the limit is replaced by a notice that keeps the comment's own
// heading, says how large the rendering was and what the cap is, tells the
// reader where to look instead, and offers the support channel.
func TestFitGitHubCommentReplacesAnOversizedBody(t *testing.T) {
	body := "## Schema Change Apply — Production\n\n" + strings.Repeat("x", 2*GitHubIssueCommentMaxChars)

	fitted, replaced := FitGitHubComment(body, "", "production")

	assert.True(t, replaced)
	assert.LessOrEqual(t, len(fitted), commentBodyLimit)
	assert.True(t, strings.HasPrefix(fitted, "## Schema Change Apply — Production\n\n"), fitted)
	assert.Contains(t, fitted, "too large to post")
	assert.Contains(t, fitted, ui.FormatNumber(int64(len(body)))+" bytes")
	assert.Contains(t, fitted, "at most 65,536")
	assert.Contains(t, fitted, "`schemabot status -e production`")
	assert.True(t, OffersSupportChannel(fitted))

	t.Run("the status hint starts with the cli name", func(t *testing.T) {
		fitted, replaced := FitGitHubComment(body, "acme schemabot", "production")
		assert.True(t, replaced)
		assert.Contains(t, fitted, "available from the CLI with `acme schemabot status -e production`.")
	})

	// A comment that names no single environment gets a placeholder the shell
	// does not read as a redirect, so the hint pasted unedited fails as an
	// unknown environment rather than as a shell syntax error.
	t.Run("an unknown environment renders a shell-safe placeholder", func(t *testing.T) {
		fitted, replaced := FitGitHubComment(body, "", "")
		assert.True(t, replaced)
		assert.Contains(t, fitted, "`schemabot status -e ENVIRONMENT`")
		assert.NotContains(t, fitted, "<environment>")
	})
}

// A body that does not open with a heading gets a notice with no title: no
// other first line is known to be safe to repeat on its own.
func TestFitGitHubCommentRepeatsOnlyAHeading(t *testing.T) {
	fitted, replaced := FitGitHubComment(strings.Repeat("| cell |\n", GitHubIssueCommentMaxChars/4), "", "")

	assert.True(t, replaced)
	assert.True(t, strings.HasPrefix(fitted, "⚠️ **This comment was too large to post.**"), fitted)
}
