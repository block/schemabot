package presentation

import (
	"regexp"
	"strings"
	"unicode"
)

// MaxErrorLen bounds an untrusted error a surface renders, so a pathological
// engine error cannot flood it. Genuine engine errors, such as a Spirit
// preflight check reason, are a few hundred characters and render intact;
// anything longer is clamped with a truncation marker.
const MaxErrorLen = 500

// InlineError is an untrusted error made safe for a single line: RedactError
// at MaxErrorLen plus collapsing whitespace runs, including line breaks. It is
// also the reason every surface groups failed members by, so the CLI and the
// PR comment put the same members together however their errors differ in
// redacted detail such as a host.
func InlineError(msg string) string {
	return strings.Join(strings.Fields(RedactError(msg, MaxErrorLen)), " ")
}

var (
	// dsnFragmentRe matches Go MySQL driver DSN fragments such as
	// user:pass@tcp(host:3306)/db, which leak credentials and endpoints.
	dsnFragmentRe = regexp.MustCompile(`\S*@tcp\([^)]*\)\S*`)
	// absolutePathRe deliberately matches only absolute paths with at least
	// two components. Relative paths and Go import paths remain useful in
	// errors and do not reveal host filesystem layout.
	absolutePathRe = regexp.MustCompile("(^|[[:space:]\\(\\\"'`=,:\\[])/[A-Za-z0-9._~+-]+(?:/[A-Za-z0-9._~+-]+)+")
	// lookupHostRe matches the hostname in net.DNSError text. The "on"
	// alternative is anchored on the resolver endpoint that follows the
	// hostname — an IP or host:port, which later passes redact — so prose
	// uses of "lookup … on …" are never matched.
	lookupHostRe = regexp.MustCompile(`\b(lookup[[:space:]]+)[A-Za-z0-9][A-Za-z0-9.-]*([[:space:]]+on[[:space:]]+(?:\d{1,3}(?:\.\d{1,3}){3}|[A-Za-z0-9][A-Za-z0-9.-]*):[0-9]{1,5}\b|:[[:space:]]+no such host)`)
	// libpqPasswordRe matches a password value in keyword/value connection
	// fragments. Unlike the other parameters, the value is bounded only by
	// whitespace (and a backtick, so backtick-quoted connection fragments
	// stay balanced): a secret may legally contain the delimiters the
	// parameter pattern stops at, and a credential must be over-redacted
	// rather than leave part of the secret behind.
	libpqPasswordRe = regexp.MustCompile("\\bpassword=[^[:space:]`]*")
	// libpqParameterRe matches identifying values in keyword/value connection
	// fragments while preserving the parameter name for diagnostic context.
	libpqParameterRe = regexp.MustCompile("\\b(user|database|dbname|host)=([^[:space:],;:`]+)(?::[0-9]+)?")
	// mysqlUserHostRe matches the MySQL account form 'user'@'host', which
	// names both a role and its origin host; the quoting is distinctive
	// enough to redact wherever it appears.
	mysqlUserHostRe = regexp.MustCompile(`'[^']*'@'[^']*'`)
	// mysqlErrorLineRe scopes quoted-identity redaction to MySQL server
	// error lines, mirroring the SQLSTATE scoping on the PostgreSQL side so
	// ordinary application errors can still quote a database name.
	mysqlErrorLineRe = regexp.MustCompile(`[^\n]*\bError [0-9]+ \([0-9A-Z]{5}\)[^\n]*`)
	mysqlIdentityRe  = regexp.MustCompile(`\b(database|user)[[:space:]]+'[^']*'`)
	// urlUserinfoRe matches credentials between a URL scheme and endpoint.
	urlUserinfoRe = regexp.MustCompile(`\b([A-Za-z][A-Za-z0-9+.-]*://)[^/@[:space:]]+@`)
	// postgresSQLSTATELineRe scopes quoted identity redaction to PostgreSQL
	// server errors, so ordinary application errors can still name a database.
	postgresSQLSTATELineRe = regexp.MustCompile(`[^\n]*\(SQLSTATE[[:space:]]+[0-9A-Z]{5}\)`)
	postgresIdentityRe     = regexp.MustCompile(`\b(user|database|dbname|host)[[:space:]]+"[^"]*"`)
	// hostPortRe matches hostnames carrying an explicit port: dotted names
	// such as db.internal.example.com:3306 and single-label service names
	// such as mysql-primary:3306, which are common in dial errors. Requiring
	// the port avoids matching schema-qualified identifiers like db.table;
	// the single-label form additionally requires a leading letter and a
	// multi-digit port so line:column references and similar numeric pairs
	// are not redacted.
	hostPortRe = regexp.MustCompile(`\b(?:(?:[A-Za-z0-9][A-Za-z0-9-]*\.)+[A-Za-z0-9][A-Za-z0-9-]*:\d{1,5}|[A-Za-z][A-Za-z0-9-]*:\d{2,5})\b`)
	// contextualHostnameRe matches any dotted hostname after connection words,
	// where the surrounding text disambiguates it from a Go symbol or prose.
	contextualHostnameRe = regexp.MustCompile(`\b((?:connect(?:ion)? to|dial(?: tcp)?)[[:space:]]+)(?:[A-Za-z0-9][A-Za-z0-9-]*\.)+[A-Za-z0-9][A-Za-z0-9-]*\b`)
	// infrastructureHostnameRe matches port-less names carrying suffixes used
	// by private DNS and managed database endpoints. Restricting the suffixes
	// avoids treating ordinary dotted words and Go symbols as hostnames.
	infrastructureHostnameRe = regexp.MustCompile(`\b(?:[A-Za-z0-9][A-Za-z0-9-]*\.)+(?:internal|rds\.amazonaws\.com)\b`)
	// ipEndpointRe matches IPv4 addresses with an optional port.
	ipEndpointRe = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b`)
	// ansiEscapeRe matches ANSI terminal escape sequences (colors, cursor
	// movement) that some tools embed in their error output.
	ansiEscapeRe = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)
)

// grpcStatusPrefixRe matches the "rpc error: code = X desc = " text a gRPC
// status adds to an error from a remote data plane. The code is transport
// detail for operators, not something a PR author acts on, and the message
// after it is the engine's own.
var grpcStatusPrefixRe = regexp.MustCompile(`rpc error: code = [A-Za-z]+ desc = `)

// RedactError makes an untrusted engine or task error safe to show on any
// operator surface: it normalizes line endings, strips control and format
// characters (including bidi overrides usable for visual spoofing), drops gRPC
// status prefixes, redacts connection details that leak credentials,
// identifiers, or internal infrastructure, trims surrounding whitespace, and
// clamps the length to limit runes. The raw error remains available
// server-side in the apply logs, so nothing is lost for triage.
func RedactError(msg string, limit int) string {
	msg = strings.ReplaceAll(msg, "\r\n", "\n")
	msg = strings.ReplaceAll(msg, "\r", "\n")
	msg = StripControlText(msg)
	msg = grpcStatusPrefixRe.ReplaceAllString(msg, "")
	msg = RedactConnectionDetails(msg)
	msg = strings.TrimSpace(msg)
	if runes := []rune(msg); len(runes) > limit {
		msg = TrimPartialRedactionMarker(string(runes[:limit-1])) + "…"
	}
	return msg
}

// RedactionMarker is the placeholder every redaction pass substitutes for
// sensitive text.
const RedactionMarker = "[endpoint redacted]"

// TrimPartialRedactionMarker removes an incomplete redaction marker left
// dangling at the end of a clamped message, so truncation can never turn a
// redaction back into text that looks like a partially leaked value.
func TrimPartialRedactionMarker(msg string) string {
	i := strings.LastIndexByte(msg, '[')
	if i < 0 {
		return msg
	}
	tail := msg[i:]
	if len(tail) < len(RedactionMarker) && strings.HasPrefix(RedactionMarker, tail) {
		return msg[:i]
	}
	return msg
}

func RedactConnectionDetails(msg string) string {
	msg = dsnFragmentRe.ReplaceAllString(msg, "[endpoint redacted]")
	msg = urlUserinfoRe.ReplaceAllString(msg, "${1}[endpoint redacted]@")
	msg = absolutePathRe.ReplaceAllString(msg, "${1}[endpoint redacted]")
	msg = lookupHostRe.ReplaceAllString(msg, "${1}[endpoint redacted]${2}")
	msg = libpqPasswordRe.ReplaceAllString(msg, "password=[endpoint redacted]")
	msg = libpqParameterRe.ReplaceAllString(msg, "${1}=[endpoint redacted]")
	msg = postgresSQLSTATELineRe.ReplaceAllStringFunc(msg, func(line string) string {
		return postgresIdentityRe.ReplaceAllString(line, `${1} "[endpoint redacted]"`)
	})
	msg = mysqlUserHostRe.ReplaceAllString(msg, "[endpoint redacted]")
	msg = mysqlErrorLineRe.ReplaceAllStringFunc(msg, func(line string) string {
		return mysqlIdentityRe.ReplaceAllString(line, "${1} '[endpoint redacted]'")
	})
	msg = hostPortRe.ReplaceAllString(msg, "[endpoint redacted]")
	msg = contextualHostnameRe.ReplaceAllString(msg, "${1}[endpoint redacted]")
	msg = infrastructureHostnameRe.ReplaceAllString(msg, "[endpoint redacted]")
	msg = ipEndpointRe.ReplaceAllString(msg, "[endpoint redacted]")
	return msg
}

// StripControlText removes ANSI escape sequences and control and format
// characters (including bidi overrides usable for visual spoofing) from
// untrusted text, keeping newlines and tabs.
func StripControlText(msg string) string {
	msg = ansiEscapeRe.ReplaceAllString(msg, "")
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, msg)
}
