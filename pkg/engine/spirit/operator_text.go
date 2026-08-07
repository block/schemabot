package spirit

import (
	"regexp"
	"strings"
	"unicode"
)

// spiritVocabulary matches Spirit's word for a schema change, including the
// "spirit migration" phrasing its own run-start line uses.
//
// The word boundaries keep identifiers such as schema_migrations intact, and
// quoted matches are excluded separately so a table actually named
// `migrations` still reads back to the operator verbatim.
var spiritVocabulary = regexp.MustCompile(`(?i)\b(?:spirit )?migrations?\b`)

// operatorText rewrites Spirit's vocabulary into SchemaBot's for text that is
// about to become operator-facing.
//
// Spirit is a library, and its own logs and errors say "migration". By the time
// one of those lines reaches an apply's log stream, or the reason a schema
// change failed reaches a PR comment, it is SchemaBot's text and has to use
// SchemaBot's word for the thing. Rewriting here — at the boundary where the
// library's text crosses into ours — keeps the vendored library untouched and
// leaves the server-side logs carrying Spirit's original wording, which is what
// anyone cross-referencing the library upstream needs.
func operatorText(s string) string {
	matches := spiritVocabulary.FindAllStringIndex(s, -1)
	if matches == nil {
		return s
	}

	var b strings.Builder
	written := 0
	for _, m := range matches {
		start, end := m[0], m[1]
		if isQuotedIdentifier(s, start, end) {
			continue
		}
		b.WriteString(s[written:start])
		b.WriteString(schemaChangeFor(s[start:end]))
		written = end
	}
	b.WriteString(s[written:])
	return b.String()
}

// schemaChangeFor renders the replacement for one match, carrying over its
// number and its leading capitalization so a rewritten line reads the way the
// original did — "Starting spirit migration" becomes "Starting schema change",
// "concurrent migrations" becomes "concurrent schema changes".
func schemaChangeFor(match string) string {
	replacement := "schema change"
	if strings.HasSuffix(strings.ToLower(match), "s") {
		replacement += "s"
	}
	if unicode.IsUpper(rune(match[0])) {
		return strings.ToUpper(replacement[:1]) + replacement[1:]
	}
	return replacement
}

// isQuotedIdentifier reports whether a match sits against a quote character, in
// which case it is a name the operator has to be able to read back exactly —
// a table called `migrations`, not a sentence about one.
func isQuotedIdentifier(s string, start, end int) bool {
	return (start > 0 && isIdentifierQuote(s[start-1])) || (end < len(s) && isIdentifierQuote(s[end]))
}

func isIdentifierQuote(c byte) bool {
	return c == '`' || c == '\'' || c == '"'
}
