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
	"errors"
	"fmt"
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

// ParsePullRequestReference reads a repository and pull request number out of
// the forms a caller already has in hand, so naming a pull request never means
// retyping one that is already on the clipboard: the browser URL, with or
// without a scheme and with whatever trailing path, query, or fragment the
// page added; the "owner/name#number" form comments and PR bodies use; and a
// bare "owner/name", which returns a zero number for callers that take the
// number separately.
//
// The host is not checked, and neither is the path in front of the repository.
// A URL that names a repository and a pull request number identifies them just
// as well from an enterprise deployment as from github.com, and refusing one
// would send the caller back to retype an address that was already unambiguous.
func ParsePullRequestReference(reference string) (repo string, pr int, err error) {
	trimmed := strings.TrimSpace(reference)
	if trimmed == "" {
		return "", 0, errors.New("name a pull request: its URL, owner/name#number, or owner/name")
	}
	if repo, pr, ok := parsePullRequestURL(trimmed); ok {
		return repo, pr, nil
	}
	if repo, number, found := strings.Cut(trimmed, "#"); found {
		if !isRepoFullName(repo) {
			return "", 0, invalidPullRequestReference(reference)
		}
		pr, err := parsePullRequestNumber(number)
		if err != nil {
			return "", 0, err
		}
		return repo, pr, nil
	}
	if !isRepoFullName(trimmed) {
		return "", 0, invalidPullRequestReference(reference)
	}
	return trimmed, 0, nil
}

// parsePullRequestURL reads the "<owner>/<name>/pull/<number>" tail every
// pull request address ends in, from the last such segment onward: the query
// and fragment a page appends carry no part of the identity, and neither does
// anything in front of the owner, so a scheme, a host, and an enterprise path
// prefix are all skipped rather than matched against.
//
// A trailing path segment ("/files", "/commits/<sha>") is likewise ignored,
// because it names a view of the pull request rather than a different one.
func parsePullRequestURL(reference string) (string, int, bool) {
	path := reference
	if cut := strings.IndexAny(path, "?#"); cut >= 0 {
		path = path[:cut]
	}
	segments := strings.Split(path, "/")
	for i := len(segments) - 2; i >= 2; i-- {
		if segments[i] != "pull" && segments[i] != "pulls" {
			continue
		}
		pr, err := parsePullRequestNumber(segments[i+1])
		if err != nil {
			return "", 0, false
		}
		owner, name := segments[i-2], segments[i-1]
		if owner == "" || name == "" {
			return "", 0, false
		}
		return owner + "/" + name, pr, true
	}
	return "", 0, false
}

// parsePullRequestNumber reads a pull request number, refusing the values that
// name no pull request. Zero is one of them: it is what an unset number
// decodes to, and admitting it would turn a missing argument into a lookup of
// a pull request that cannot exist.
func parsePullRequestNumber(number string) (int, error) {
	pr, err := strconv.Atoi(strings.TrimSpace(number))
	if err != nil {
		return 0, fmt.Errorf("read %q as a pull request number: %w", number, err)
	}
	if pr <= 0 {
		return 0, fmt.Errorf("pull request number must be positive, got %d", pr)
	}
	return pr, nil
}

// isRepoFullName reports whether a string is an "owner/name" pair. Neither
// half may be empty, and a third segment means the string is a path rather
// than a repository.
func isRepoFullName(s string) bool {
	owner, name, found := strings.Cut(s, "/")
	return found && owner != "" && name != "" && !strings.Contains(name, "/")
}

func invalidPullRequestReference(reference string) error {
	return fmt.Errorf("read %q as a pull request: name it by URL, owner/name#number, or owner/name", reference)
}
