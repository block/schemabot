// Package vschema compares and renders diffs of Vitess VSchema JSON
// documents. Comparison is semantic: whitespace, key ordering, and proto
// zero-value fields (e.g. "sharded": false) are ignored, matching how Vitess
// stores VSchema. Any Vitess-family engine can use these helpers to decide
// whether a desired VSchema differs from the live one and to render the
// change for operators.
package vschema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
	"google.golang.org/protobuf/encoding/protojson"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// Changed returns true if the current and desired VSchema JSON strings
// differ semantically (ignoring whitespace and key ordering).
func Changed(current, desired string) bool {
	current = Normalize(current)
	desired = Normalize(desired)
	return current != desired
}

// Diff returns a unified diff between the current and desired VSchema
// JSON strings. Returns empty string if they are identical.
func Diff(current, desired string) string {
	currentPretty := prettyJSON(Normalize(current))
	desiredPretty := prettyJSON(Normalize(desired))

	if currentPretty == desiredPretty {
		return ""
	}

	return unifiedDiff(difflib.SplitLines(currentPretty), difflib.SplitLines(desiredPretty), 3)
}

// unifiedDiff renders a unified diff of a against b with the given lines of
// context. Each added or removed block is slid as far down as it can go
// without changing the result, the way git places it. Without the slide, a
// table added below one with the same body reads as an edit to the table
// above it: the added lines would open inside that table, at its first body
// line, instead of at the new table's own name.
func unifiedDiff(a, b []string, context int) string {
	codes := slideChangesDown(difflib.NewMatcher(a, b).GetOpCodes(), a, b)
	var sb strings.Builder
	sb.WriteString("--- current\n+++ new\n")
	for _, group := range groupOpCodes(codes, context) {
		first, last := group[0], group[len(group)-1]
		fmt.Fprintf(&sb, "@@ -%s +%s @@\n", unifiedRange(first.I1, last.I2), unifiedRange(first.J1, last.J2))
		for _, c := range group {
			if c.Tag == 'e' {
				writePrefixed(&sb, " ", a[c.I1:c.I2])
				continue
			}
			if c.Tag == 'r' || c.Tag == 'd' {
				writePrefixed(&sb, "-", a[c.I1:c.I2])
			}
			if c.Tag == 'r' || c.Tag == 'i' {
				writePrefixed(&sb, "+", b[c.J1:c.J2])
			}
		}
	}
	return sb.String()
}

// slideChangesDown moves each pure insertion or deletion down one line at a
// time while the line just past it equals its first line, which leaves both
// documents' lines matched the same way. It stops one line short of the next
// change so two changes never merge.
func slideChangesDown(codes []difflib.OpCode, a, b []string) []difflib.OpCode {
	for k := 0; k < len(codes); k++ {
		if codes[k].Tag != 'i' && codes[k].Tag != 'd' {
			continue
		}
		for k+1 < len(codes) && codes[k+1].Tag == 'e' && codes[k+1].I2-codes[k+1].I1 > 1 && canSlideDown(codes[k], a, b) {
			if k == 0 || codes[k-1].Tag != 'e' {
				c := codes[k]
				codes = append(codes[:k], append([]difflib.OpCode{{Tag: 'e', I1: c.I1, I2: c.I1, J1: c.J1, J2: c.J1}}, codes[k:]...)...)
				k++
			}
			codes[k-1].I2++
			codes[k-1].J2++
			codes[k].I1++
			codes[k].I2++
			codes[k].J1++
			codes[k].J2++
			codes[k+1].I1++
			codes[k+1].J1++
		}
	}
	return codes
}

// canSlideDown reports whether the change's first line equals the line just
// past it, so moving the change down one line yields the same documents.
func canSlideDown(c difflib.OpCode, a, b []string) bool {
	if c.Tag == 'i' {
		return b[c.J1] == b[c.J2]
	}
	return a[c.I1] == a[c.I2]
}

// groupOpCodes splits codes into hunks with up to context lines of unchanged
// text around each change, the way a unified diff groups them.
func groupOpCodes(codes []difflib.OpCode, context int) [][]difflib.OpCode {
	if len(codes) == 0 {
		return nil
	}
	if c := codes[0]; c.Tag == 'e' {
		codes[0] = difflib.OpCode{Tag: 'e', I1: max(c.I1, c.I2-context), I2: c.I2, J1: max(c.J1, c.J2-context), J2: c.J2}
	}
	if c := codes[len(codes)-1]; c.Tag == 'e' {
		codes[len(codes)-1] = difflib.OpCode{Tag: 'e', I1: c.I1, I2: min(c.I2, c.I1+context), J1: c.J1, J2: min(c.J2, c.J1+context)}
	}
	var groups [][]difflib.OpCode
	var group []difflib.OpCode
	for _, c := range codes {
		if c.Tag == 'e' && c.I2-c.I1 > 2*context {
			group = append(group, difflib.OpCode{Tag: 'e', I1: c.I1, I2: min(c.I2, c.I1+context), J1: c.J1, J2: min(c.J2, c.J1+context)})
			groups = append(groups, group)
			group = nil
			c.I1, c.J1 = max(c.I1, c.I2-context), max(c.J1, c.J2-context)
		}
		group = append(group, c)
	}
	if len(group) > 0 && (len(group) != 1 || group[0].Tag != 'e') {
		groups = append(groups, group)
	}
	return groups
}

// unifiedRange formats a hunk's line range as a unified diff header does.
func unifiedRange(start, stop int) string {
	first, length := start+1, stop-start
	if length == 1 {
		return fmt.Sprintf("%d", first)
	}
	if length == 0 {
		first--
	}
	return fmt.Sprintf("%d,%d", first, length)
}

// writePrefixed writes each line with prefix in front of it.
func writePrefixed(sb *strings.Builder, prefix string, lines []string) {
	for _, line := range lines {
		sb.WriteString(prefix)
		sb.WriteString(line)
	}
}

// Normalize canonicalizes a VSchema JSON string for comparison.
// Uses Vitess protobuf round-trip to strip proto zero-value fields (e.g.,
// "sharded": false) that are semantically equivalent to being absent.
// This matches the approach used for VSchema comparison in the tern layer.
func Normalize(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "{}"
	}
	// Round-trip through Vitess vschemapb.Keyspace to strip zero-value fields.
	// protojson.Marshal with EmitUnpopulated=false (the default) omits fields
	// like "sharded": false, which Vitess strips when storing VSchema.
	var ks vschemapb.Keyspace
	if err := protojson.Unmarshal([]byte(s), &ks); err == nil {
		marshaler := protojson.MarshalOptions{UseProtoNames: true}
		if data, err := marshaler.Marshal(&ks); err == nil {
			// protojson map ordering is non-deterministic. Re-marshal through
			// encoding/json which sorts map keys for stable comparison.
			return sortedJSON(string(data))
		}
	}

	// Fallback: JSON round-trip stripping empty maps
	return sortedJSON(s)
}

// sortedJSON re-marshals JSON with sorted keys for deterministic comparison.
// Strips empty maps as a side effect of the round-trip.
func sortedJSON(s string) string {
	var obj any
	if err := json.Unmarshal([]byte(s), &obj); err != nil {
		return s
	}
	data, err := json.Marshal(obj)
	if err != nil {
		return s
	}
	return string(data)
}

// prettyJSON formats JSON with 2-space indentation.
func prettyJSON(s string) string {
	if s == "" {
		return "{}\n"
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, []byte(s), "", "  "); err != nil {
		return s
	}
	buf.WriteByte('\n')
	return buf.String()
}
