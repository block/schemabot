package templates

import (
	"fmt"
	"html"
	"strings"
	"time"
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

// writeErrorBlock writes an error message as a blockquote with warning emoji.
func writeErrorBlock(sb *strings.Builder, msg string) {
	fmt.Fprintf(sb, "\n> ⚠️ **Error:** %s\n", msg)
}

// writeTableErrorLine writes a task's last error as a blockquote below its
// progress line. Newlines are quoted so multi-line engine errors stay inside
// the quote instead of escaping into the surrounding comment structure.
func writeTableErrorLine(sb *strings.Builder, msg string) {
	quoted := strings.ReplaceAll(msg, "\n", "\n> ")
	fmt.Fprintf(sb, "> ⚠️ Last error: %s\n", quoted)
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
