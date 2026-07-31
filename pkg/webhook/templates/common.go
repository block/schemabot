package templates

import (
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"
	"unicode"
)

// capitalizeFirst capitalizes the first letter of a string.
func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// humanizeState renders a canonical snake_case state constant as a
// human-readable label (e.g. "waiting_for_cutover" → "Waiting for cutover").
// Used by default branches so a state without an explicit template never
// leaks a raw constant into a PR comment.
func humanizeState(s string) string {
	return capitalizeFirst(strings.ReplaceAll(s, "_", " "))
}

func environmentTitleSuffix(environment string) string {
	environment = strings.TrimSpace(environment)
	if environment == "" {
		return ""
	}
	return " — " + capitalizeFirst(environment)
}

func titleLabel(value string) string {
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == '-' || r == '_' || r == ' '
	})
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
	}
	return strings.Join(parts, " ")
}

func writeEnvironmentTitle(sb *strings.Builder, title, environment string) {
	fmt.Fprintf(sb, "## %s%s\n\n", title, environmentTitleSuffix(environment))
}

// TimestampFunc is the function used to generate timestamps in templates.
// Override in previews/tests to produce deterministic output.
var TimestampFunc = func() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05 UTC")
}

// NowFunc returns the current time. Override in previews for deterministic output.
var NowFunc = time.Now

// SupportChannelData configures an optional support destination shown in PR comments.
type SupportChannelData struct {
	Name string
	URL  string
}

// currentTimestamp returns the current UTC time formatted for PR comments.
func currentTimestamp() string {
	return TimestampFunc()
}

// supportChannelOfferMarker flags a rendered comment as reporting a problem
// the author may need help resolving. GitHub never renders HTML comments, so
// the marker is invisible on the PR. Render functions declare footer
// eligibility explicitly with offerSupportChannel/writeSupportChannelOffer;
// the webhook layer appends the support-channel footer to any comment that
// carries the marker, without inspecting human-visible copy.
const supportChannelOfferMarker = "<!-- schemabot:offer-support-channel -->"

// OffersSupportChannel reports whether a rendered comment declared itself
// eligible for the support-channel footer. Embedded sections keep their
// marker, so an aggregate comment containing a failed per-deployment section
// offers support just like the standalone comment would.
func OffersSupportChannel(body string) bool {
	return containsSupportChannelMarkerLine(body)
}

// containsSupportChannelMarkerLine matches the marker only as a standalone
// line, the way offerSupportChannel and writeSupportChannelOffer emit it.
// User-controlled content rendered into a comment (DDL, error detail) that
// happens to contain the marker text mid-line neither triggers the footer
// nor suppresses a render function's own declaration.
func containsSupportChannelMarkerLine(body string) bool {
	return strings.HasPrefix(body, supportChannelOfferMarker+"\n") ||
		strings.Contains(body, "\n"+supportChannelOfferMarker+"\n") ||
		strings.HasSuffix(body, "\n"+supportChannelOfferMarker)
}

// offerSupportChannel appends the support marker to a rendered comment body.
func offerSupportChannel(body string) string {
	if containsSupportChannelMarkerLine(body) {
		return body
	}
	return strings.TrimRight(body, "\n") + "\n" + supportChannelOfferMarker + "\n"
}

// writeSupportChannelOffer writes the support marker into a comment being
// composed, for builders that decide eligibility mid-composition (e.g. on a
// failed status line). At most one marker is written per comment.
func writeSupportChannelOffer(sb *strings.Builder) {
	if containsSupportChannelMarkerLine(sb.String()) {
		return
	}
	if sb.Len() > 0 && !strings.HasSuffix(sb.String(), "\n") {
		sb.WriteString("\n")
	}
	sb.WriteString(supportChannelOfferMarker + "\n")
}

// RenderSupportChannelFooter appends a support-channel footer to a rendered PR comment.
func RenderSupportChannelFooter(body string, support SupportChannelData) string {
	if support.Name == "" || support.URL == "" {
		return body
	}
	footer := fmt.Sprintf("> 💬 Support: [%s](%s).", escapeMarkdownLinkText(support.Name), support.URL)
	if strings.Contains(body, footer) {
		return body
	}
	return strings.TrimRight(body, "\n") + "\n\n" + footer
}

func escapeMarkdownLinkText(text string) string {
	text = strings.ReplaceAll(text, "\\", "\\\\")
	return strings.ReplaceAll(text, "]", "\\]")
}

// maxCommentErrorLen bounds an error message rendered into a PR comment so a
// pathological engine error cannot flood the comment. Genuine engine errors,
// such as a Spirit preflight check reason, are a few hundred characters and
// render intact; anything longer is clamped with a truncation marker.
const maxCommentErrorLen = 500

var (
	// dsnFragmentRe matches Go MySQL driver DSN fragments such as
	// user:pass@tcp(host:3306)/db, which leak credentials and endpoints.
	dsnFragmentRe = regexp.MustCompile(`\S*@tcp\([^)]*\)\S*`)
	// hostPortRe matches dotted hostnames carrying an explicit port, such as
	// db.internal.example.com:3306. Requiring the port avoids matching
	// schema-qualified identifiers like db.table.
	hostPortRe = regexp.MustCompile(`\b(?:[A-Za-z0-9][A-Za-z0-9-]*\.)+[A-Za-z0-9][A-Za-z0-9-]*:\d{1,5}\b`)
	// ipEndpointRe matches IPv4 addresses with an optional port.
	ipEndpointRe = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b`)
	// ansiEscapeRe matches ANSI terminal escape sequences (colors, cursor
	// movement) that some tools embed in their error output.
	ansiEscapeRe = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)
)

// sanitizeCommentError makes an untrusted engine or task error safe to render
// in a public PR comment: it normalizes line endings, strips control
// characters, redacts connection endpoints (DSN fragments, host:port pairs,
// IP addresses) that leak internal infrastructure, trims surrounding
// whitespace, and clamps the length by rune count. The raw error remains
// available server-side in the apply logs, so nothing is lost for triage.
func sanitizeCommentError(msg string) string {
	msg = strings.ReplaceAll(msg, "\r\n", "\n")
	msg = strings.ReplaceAll(msg, "\r", "\n")
	msg = ansiEscapeRe.ReplaceAllString(msg, "")
	msg = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, msg)
	msg = dsnFragmentRe.ReplaceAllString(msg, "[endpoint redacted]")
	msg = hostPortRe.ReplaceAllString(msg, "[endpoint redacted]")
	msg = ipEndpointRe.ReplaceAllString(msg, "[endpoint redacted]")
	msg = strings.TrimSpace(msg)
	if runes := []rune(msg); len(runes) > maxCommentErrorLen {
		msg = string(runes[:maxCommentErrorLen-1]) + "…"
	}
	return msg
}

// quoteBlockLines keeps a multi-line message inside a Markdown blockquote by
// prefixing continuation lines with the quote marker, so engine errors cannot
// escape into the surrounding comment structure.
func quoteBlockLines(msg string) string {
	return strings.ReplaceAll(msg, "\n", "\n> ")
}

// writeErrorBlock writes an error message as a blockquote with warning emoji.
// The message is sanitized before rendering; a message that sanitizes to
// empty writes nothing.
func writeErrorBlock(sb *strings.Builder, msg string) {
	sanitized := sanitizeCommentError(msg)
	if sanitized == "" {
		return
	}
	fmt.Fprintf(sb, "\n> ⚠️ **Error:** %s\n", quoteBlockLines(sanitized))
}

// writeTableErrorLine writes a task's last error as a blockquote below its
// progress line. The message is sanitized before rendering; a message that
// sanitizes to empty writes nothing.
func writeTableErrorLine(sb *strings.Builder, msg string) {
	sanitized := sanitizeCommentError(msg)
	if sanitized == "" {
		return
	}
	fmt.Fprintf(sb, "> ⚠️ Last error: %s\n", quoteBlockLines(sanitized))
}

// taskErrorAddsDetail reports whether a failed table's own error message adds
// information beyond the apply-level error block rendered elsewhere in the
// comment. The apply's failure reason is promoted from its first failed task,
// so for the common single-table failure the two messages are identical and
// repeating the text below the row would only add noise. Both messages are
// compared in their sanitized form — the representation actually rendered —
// so differences that sanitization erases cannot defeat the suppression.
func taskErrorAddsDetail(taskError, applyError string) bool {
	sanitized := sanitizeCommentError(taskError)
	return sanitized != "" && sanitized != sanitizeCommentError(applyError)
}

// writeSuccessBlock writes a success message as a blockquote.
func writeSuccessBlock(sb *strings.Builder, msg string) {
	fmt.Fprintf(sb, "\n> %s\n", msg)
}

// writeRequesterOrTimestamp writes the requester attribution or a start timestamp.
// Leading blank line ensures it renders as a separate line in GitHub markdown.
func writeRequesterOrTimestamp(sb *strings.Builder, requestedBy string) {
	writeAttributionLine(sb, "Requested", requestedBy)
}

func writeAppliedByOrTimestampAt(sb *strings.Builder, appliedBy, timestamp string) {
	writeAttributionLineAt(sb, "Applied", appliedBy, timestamp, "")
}

// writeAttributionLine writes a "*Verb by @user at timestamp*" line.
func writeAttributionLine(sb *strings.Builder, verb, user string) {
	writeAttributionLineWithSuffix(sb, verb, user, "")
}

func writeAttributionLineWithSuffix(sb *strings.Builder, verb, user, suffix string) {
	writeAttributionLineAt(sb, verb, user, currentTimestamp(), suffix)
}

func writeAttributionLineAt(sb *strings.Builder, verb, user, timestamp, suffix string) {
	if user != "" {
		fmt.Fprintf(sb, "\n*%s by @%s at %s%s*\n", verb, user, timestamp, suffix)
	} else {
		fmt.Fprintf(sb, "\n*Started at %s%s*\n", timestamp, suffix)
	}
}

func writeLastUpdatedFooter(sb *strings.Builder, timestamp string) {
	if !strings.HasSuffix(sb.String(), "\n") {
		sb.WriteString("\n")
	}
	escapedTimestamp := html.EscapeString(timestamp)
	fmt.Fprintf(sb, "\n_Last updated: <relative-time datetime=\"%s\">%s</relative-time> (%s)_\n",
		escapeRelativeTimeDatetime(timestamp),
		escapedTimestamp,
		escapedTimestamp)
}

func escapeRelativeTimeDatetime(timestamp string) string {
	parsed, err := time.Parse("2006-01-02 15:04:05 UTC", timestamp)
	if err != nil {
		return html.EscapeString(timestamp)
	}
	return parsed.UTC().Format(time.RFC3339)
}

// writeDBEnvLine writes the **Database** | **Environment** metadata line.
func writeDBEnvLine(sb *strings.Builder, database, environment string) {
	if environment != "" {
		fmt.Fprintf(sb, "**Database**: `%s` | **Environment**: `%s`\n", database, environment)
	} else {
		fmt.Fprintf(sb, "**Database**: `%s`\n", database)
	}
}

func writeDBLine(sb *strings.Builder, database string) {
	if database == "" {
		return
	}
	fmt.Fprintf(sb, "**Database**: `%s`\n", database)
}
