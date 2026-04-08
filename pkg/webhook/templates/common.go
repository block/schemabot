package templates

import (
	"fmt"
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

// TimestampFunc is the function used to generate timestamps in templates.
// Override in previews/tests to produce deterministic output.
var TimestampFunc = func() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05 UTC")
}

// NowFunc returns the current time. Override in previews for deterministic output.
var NowFunc = time.Now

// currentTimestamp returns the current UTC time formatted for PR comments.
func currentTimestamp() string {
	return TimestampFunc()
}

// writeErrorBlock writes an error message as a blockquote with warning emoji.
func writeErrorBlock(sb *strings.Builder, msg string) {
	fmt.Fprintf(sb, "\n> ⚠️ **Error:** %s\n", msg)
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

// writeAppliedByOrTimestamp writes "Applied by" attribution for apply/progress comments.
func writeAppliedByOrTimestamp(sb *strings.Builder, appliedBy string) {
	writeAttributionLine(sb, "Applied", appliedBy)
}

// writeAttributionLine writes a "*Verb by @user at timestamp*" line.
func writeAttributionLine(sb *strings.Builder, verb, user string) {
	ts := currentTimestamp()
	if user != "" {
		fmt.Fprintf(sb, "\n*%s by @%s at %s*\n", verb, user, ts)
	} else {
		fmt.Fprintf(sb, "\n*Started at %s*\n", ts)
	}
}

// truncateDDL strips backtick-quoting from a DDL statement and truncates to maxLen characters.
// Used in summary comments where DDL should be inline and scannable, not verbose.
func truncateDDL(ddl string, maxLen int) string {
	cleaned := strings.ReplaceAll(ddl, "`", "")
	if len(cleaned) > maxLen {
		return cleaned[:maxLen-3] + "..."
	}
	return cleaned
}

// writeDBEnvLine writes the **Database** | **Environment** metadata line.
func writeDBEnvLine(sb *strings.Builder, database, environment string) {
	if environment != "" {
		fmt.Fprintf(sb, "**Database**: `%s` | **Environment**: `%s`\n", database, environment)
	} else {
		fmt.Fprintf(sb, "**Database**: `%s`\n", database)
	}
}
