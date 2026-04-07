package templates

import (
	"fmt"
	"strings"
)

// ANSI styling codes
const (
	ANSIBold    = "\033[1m"
	ANSICyan    = "\033[36m"
	ANSIGreen   = "\033[32m"
	ANSIBlue    = "\033[34m"
	ANSIMagenta = "\033[35m"
	ANSIRed     = "\033[31m"
	ANSIOrange  = "\033[38;5;208m"
	ANSIReset   = "\033[0m"
	ANSIDim     = "\033[2m"
	ANSIYellow  = "\033[33m"
)

// StopKeyHint is the label shown to users for the stop keybinding.
const StopKeyHint = "s stop"

// SQL keywords to highlight
var sqlKeywords = []string{
	"CREATE", "TABLE", "ALTER", "DROP", "INDEX", "ADD", "KEY",
	"PRIMARY", "UNIQUE", "FOREIGN", "REFERENCES", "NOT", "NULL",
	"DEFAULT", "AUTO_INCREMENT", "ENGINE", "CHARSET", "COLLATE",
	"VARCHAR", "BIGINT", "INT", "TEXT", "TIMESTAMP", "BOOLEAN",
	"ON", "UPDATE", "CURRENT_TIMESTAMP", "IF", "EXISTS",
}

// FormatSQL applies syntax highlighting to a SQL statement.
// Keywords are highlighted in blue, table name in magenta.
func FormatSQL(sql string) string {
	result := sql

	// First, highlight the table name (comes after "TABLE ") in cyan
	result = highlightTableName(result)

	// Then highlight keywords in blue
	for _, keyword := range sqlKeywords {
		// Match keyword as whole word (case insensitive)
		// Use simple replacement - keywords in backticks won't match
		result = highlightKeyword(result, keyword)
	}
	return result
}

// highlightTableName highlights the table name after "TABLE " in magenta.
func highlightTableName(s string) string {
	upper := strings.ToUpper(s)

	// Find "TABLE " (with space after)
	idx := strings.Index(upper, "TABLE ")
	if idx == -1 {
		return s
	}

	// Skip past "TABLE "
	tableStart := idx + 6

	// Find the table name - it's either backtick-quoted or ends at space/paren
	if tableStart >= len(s) {
		return s
	}

	var tableName string
	var tableEnd int

	if s[tableStart] == '`' {
		// Backtick-quoted: find closing backtick
		closeBacktick := strings.Index(s[tableStart+1:], "`")
		if closeBacktick == -1 {
			return s
		}
		tableEnd = tableStart + 1 + closeBacktick + 1
		tableName = s[tableStart:tableEnd] // Include backticks
	} else {
		// Unquoted: find end (space or paren)
		tableEnd = tableStart
		for tableEnd < len(s) && s[tableEnd] != ' ' && s[tableEnd] != '(' {
			tableEnd++
		}
		tableName = s[tableStart:tableEnd]
	}

	// Build result with highlighted table name
	var result strings.Builder
	result.WriteString(s[:tableStart])
	result.WriteString(ANSIMagenta)
	result.WriteString(tableName)
	result.WriteString(ANSIReset)
	result.WriteString(s[tableEnd:])

	return result.String()
}

// highlightKeyword highlights a SQL keyword in the string.
func highlightKeyword(s, keyword string) string {
	// Simple case-insensitive word boundary replacement
	keywordUpper := strings.ToUpper(keyword)

	var result strings.Builder
	i := 0
	for i < len(s) {
		idx := strings.Index(strings.ToUpper(s[i:]), keywordUpper)
		if idx == -1 {
			result.WriteString(s[i:])
			break
		}

		// Check word boundaries
		absIdx := i + idx
		isWordStart := absIdx == 0 || !isAlphaNum(s[absIdx-1])
		isWordEnd := absIdx+len(keyword) >= len(s) || !isAlphaNum(s[absIdx+len(keyword)])

		if isWordStart && isWordEnd {
			result.WriteString(s[i : i+idx])
			result.WriteString(ANSIBlue)
			result.WriteString(s[absIdx : absIdx+len(keyword)])
			result.WriteString(ANSIReset)
			i = absIdx + len(keyword)
		} else {
			result.WriteString(s[i : i+idx+1])
			i = i + idx + 1
		}
	}
	return result.String()
}

func isAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// FormatStatusLine returns a styled status line with optional ETA.
// Example output: "🔄 Copying rows... ETA 5m 30s"
func FormatStatusLine(status, eta string) string {
	styled := fmt.Sprintf("%s🔄 %s%s", ANSIBold, status, ANSIReset)
	if eta != "" && eta != "TBD" {
		styled += fmt.Sprintf(" %sETA %s%s", ANSIDim, strings.TrimSpace(eta), ANSIReset)
	}
	return styled
}

// FormatApplyComplete returns the styled "Apply complete" message.
func FormatApplyComplete() string {
	return fmt.Sprintf("%s%s✓ Apply complete!%s", ANSIBold, ANSIGreen, ANSIReset)
}

// FormatApplyStopped returns the styled "Apply stopped" message.
func FormatApplyStopped() string {
	return fmt.Sprintf("%s%s⏹️ Apply stopped%s", ANSIBold, ANSIYellow, ANSIReset)
}

// FormatWatchFooter returns the footer shown during apply watch mode.
func FormatWatchFooter() string {
	return fmt.Sprintf("%sESC detach • %s • v volume%s", ANSIDim, StopKeyHint, ANSIReset)
}

func colorWrap(code string) func(string) string {
	return func(s string) string {
		return code + s + ANSIReset
	}
}
