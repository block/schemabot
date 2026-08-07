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

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(currentPretty),
		B:        difflib.SplitLines(desiredPretty),
		FromFile: "current",
		ToFile:   "new",
		Context:  3,
	}
	// The two documents differ at this point, so an empty result would be
	// indistinguishable from "no change". Surface rendering failure with a
	// fixed placeholder instead (never the raw error, which could carry
	// internal detail into PR-facing output).
	text, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return "(VSchema diff unavailable)"
	}
	return text
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
