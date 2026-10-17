package webhook

import (
	"fmt"
	"regexp"
	"strings"
)

// githubLogin matches the shape of a GitHub login: letters, digits, and
// hyphens, at most 39 characters, with the "[bot]" suffix app accounts carry.
var githubLogin = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,38}(\[bot\])?$`)

// formatGitHubCaller builds the Caller string stored on applies and control
// requests for a PR command: "github:<user>@<repo>#<pr>". actorFromCaller is its
// inverse — keep the two in sync.
func formatGitHubCaller(user, repo string, pr int) string {
	return fmt.Sprintf("github:%s@%s#%d", user, repo, pr)
}

// callerIsPRCommand reports whether caller is a command posted on the given
// PR, as formatGitHubCaller records it.
func callerIsPRCommand(caller, repo string, pr int) bool {
	actor := actorFromCaller(caller)
	return actor != "" && caller == formatGitHubCaller(actor, repo, pr)
}

// actorFromCaller extracts the GitHub username from a Caller produced by
// formatGitHubCaller. It returns "" for any other shape, so display code falls
// back to a non-attributed line rather than rendering the raw structured caller.
// A caller can arrive from an API client rather than from a PR command, so a
// username that is not a well-formed GitHub login also yields "": it is
// interpolated into PR markdown, and must not carry markup into it (AV-8).
func actorFromCaller(caller string) string {
	rest, ok := strings.CutPrefix(caller, "github:")
	if !ok {
		return ""
	}
	at := strings.IndexByte(rest, '@')
	if at <= 0 {
		return ""
	}
	if !githubLogin.MatchString(rest[:at]) {
		return ""
	}
	return rest[:at]
}
