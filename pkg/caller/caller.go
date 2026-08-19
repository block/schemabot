// Package caller owns the wire format of SchemaBot's caller attribution
// strings, so the format is produced and parsed in exactly one place.
//
// A caller records how an operation was driven: the CLI produces
// "cli:<user>@<host>", the webhook path produces "github:<user>@<repo>#<pr>",
// and an authenticated direct-API request is attributed to the bare subject.
// The user portion may itself contain "@" — authenticated subjects are often
// emails — while the trailing location (a hostname, or a repo#pr) never does,
// so parsers split at the last "@".
package caller

import (
	"strconv"
	"strings"
)

// CLIPrefix is the channel prefix on CLI-driven caller attributions.
const CLIPrefix = "cli:"

// GitHubPrefix is the channel prefix on webhook-driven caller attributions.
const GitHubPrefix = "github:"

// maxHostChars caps a hostname at the DNS length limit for a fully qualified
// domain name.
const maxHostChars = 253

// FormatCLI renders the CLI caller attribution "cli:<user>@<host>".
func FormatCLI(user, host string) string {
	return CLIPrefix + user + "@" + host
}

// SplitCLI splits a CLI-shaped caller into its user and host segments. The
// host is the segment after the last "@". It returns ok=false when the caller
// is not CLI-shaped or carries no host, so non-CLI callers can pass through
// untouched.
func SplitCLI(caller string) (user, host string, ok bool) {
	rest, found := strings.CutPrefix(caller, CLIPrefix)
	if !found {
		return "", "", false
	}
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return "", "", false
	}
	return rest[:at], rest[at+1:], true
}

// ValidHost reports whether a client-supplied hostname is safe to store and
// render. Stored callers are rendered raw in the CLI detail view, so the
// hostname is restricted to hostname-shaped characters — no whitespace,
// control characters, or terminal escapes — and bounded in length.
func ValidHost(host string) bool {
	if host == "" || len(host) > maxHostChars {
		return false
	}
	for _, r := range host {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// PullRequest extracts the repository and pull request number from a
// webhook-shaped caller "github:<user>@<owner>/<repo>#<pr>". The location is
// the segment after the last "@" — matching how Short cuts, so email-shaped
// users parse correctly. It returns ok=false when the caller is not
// webhook-shaped or its location does not carry a repo#pr, so CLI and
// bare-subject callers pass through untouched.
func PullRequest(caller string) (repo string, pr int, ok bool) {
	rest, found := strings.CutPrefix(caller, GitHubPrefix)
	if !found {
		return "", 0, false
	}
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return "", 0, false
	}
	repo, prText, found := strings.Cut(rest[at+1:], "#")
	if !found || !strings.Contains(repo, "/") {
		return "", 0, false
	}
	n, err := strconv.Atoi(prText)
	if err != nil || n <= 0 {
		return "", 0, false
	}
	return repo, n, true
}

// Short strips the trailing location from a caller for compact display and
// PR-facing surfaces: the machine for CLI callers ("cli:jdoe@macbook.local"
// -> "cli:jdoe") and the repo#pr for webhook callers
// ("github:jdoe@org/repo#42" -> "github:jdoe"). Because the cut is at the
// last "@", an email-shaped user keeps its domain
// ("cli:jdoe@example.com@macbook.local" -> "cli:jdoe@example.com"). A caller
// with no location passes through unchanged.
func Short(caller string) string {
	if at := strings.LastIndex(caller, "@"); at >= 0 {
		return caller[:at]
	}
	return caller
}
