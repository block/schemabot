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

// PullRequest extracts the repository and pull request number from an apply's
// caller attribution. It accepts the webhook wire format
// "github:<user>@<owner>/<repo>#<pr>" — the location is the segment after the
// last "@", matching how Short cuts, so email-shaped users parse correctly —
// and the bare "<owner>/<repo>#<pr>" form the server substitutes when an
// apply carries PR provenance but no recorded caller. It returns ok=false
// otherwise, so CLI and bare-subject callers pass through untouched.
func PullRequest(caller string) (repo string, pr int, ok bool) {
	if rest, found := strings.CutPrefix(caller, GitHubPrefix); found {
		at := strings.LastIndex(rest, "@")
		if at < 0 {
			return "", 0, false
		}
		return splitRepoPR(rest[at+1:])
	}
	if strings.ContainsAny(caller, ":@") {
		return "", 0, false
	}
	return splitRepoPR(caller)
}

// splitRepoPR splits an "<owner>/<repo>#<pr>" location into its repository
// and PR number. The repository must be exactly owner/name — one slash, each
// segment restricted to repository-shaped characters — and the PR number a
// strictly positive plain decimal, so a malformed location never yields a
// bogus PR link.
func splitRepoPR(location string) (repo string, pr int, ok bool) {
	repo, prText, found := strings.Cut(location, "#")
	if !found {
		return "", 0, false
	}
	owner, name, found := strings.Cut(repo, "/")
	if !found || !validRepoSegment(owner) || !validRepoSegment(name) {
		return "", 0, false
	}
	n, ok := parsePRNumber(prText)
	if !ok {
		return "", 0, false
	}
	return repo, n, true
}

// validRepoSegment reports whether an owner or repository-name segment is
// safe to render as part of a PR link. Like ValidHost, the concern is that
// callers reach the CLI display raw: the segment is restricted to the
// characters GitHub allows in owners and repository names — no whitespace,
// control characters, terminal escapes, or a second slash.
func validRepoSegment(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// parsePRNumber parses a pull request number written as a strictly positive
// plain decimal: ASCII digits only, no sign, no leading zero. Anything looser
// (such as "+5" or "007") would render a link the location does not literally
// name, so it is refused instead.
func parsePRNumber(s string) (int, bool) {
	if s == "" || s[0] == '0' {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
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

// PullRequestURL renders the canonical github.com URL for a pull request.
// Every surface that links a repo/PR pair — API responses, PR comment
// markdown, CLI tables — builds the address here so the rendering never
// drifts between them.
func PullRequestURL(repo string, pr int) string {
	return "https://github.com/" + repo + "/pull/" + strconv.Itoa(pr)
}

// PullRequestMarkdownLink renders a pull request as a "repo#N" markdown link
// to its canonical github.com URL, the form PR comments use to name another
// pull request.
func PullRequestMarkdownLink(repo string, pr int) string {
	return "[" + repo + "#" + strconv.Itoa(pr) + "](" + PullRequestURL(repo, pr) + ")"
}
