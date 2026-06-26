package webhook

import (
	"fmt"
	"strings"
)

// formatGitHubCaller builds the Caller string stored on applies and control
// requests for a PR command: "github:<user>@<repo>#<pr>". actorFromCaller is its
// inverse — keep the two in sync.
func formatGitHubCaller(user, repo string, pr int) string {
	return fmt.Sprintf("github:%s@%s#%d", user, repo, pr)
}

// actorFromCaller extracts the GitHub username from a Caller produced by
// formatGitHubCaller. It returns "" for any other shape, so display code falls
// back to a non-attributed line rather than rendering the raw structured caller.
func actorFromCaller(caller string) string {
	rest, ok := strings.CutPrefix(caller, "github:")
	if !ok {
		return ""
	}
	if at := strings.IndexByte(rest, '@'); at > 0 {
		return rest[:at]
	}
	return ""
}
