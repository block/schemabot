package planetscale

import (
	"bytes"
	"encoding/json"

	"github.com/pmezard/go-difflib/difflib"
	"google.golang.org/protobuf/encoding/protojson"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// VSchemaChanged returns true if the current and desired VSchema JSON strings
// differ semantically (ignoring whitespace and key ordering).
func VSchemaChanged(current, desired string) bool {
	current = normalizeVSchemaJSON(current)
	desired = normalizeVSchemaJSON(desired)
	return current != desired
}

// VSchemaDiff returns a unified diff between the current and desired VSchema
// JSON strings. Returns empty string if they are identical.
func VSchemaDiff(current, desired string) string {
	currentPretty := prettyJSON(normalizeVSchemaJSON(current))
	desiredPretty := prettyJSON(normalizeVSchemaJSON(desired))

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
	text, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return ""
	}
	return text
}

// normalizeVSchemaJSON normalizes a VSchema JSON string for comparison.
// Uses Vitess protobuf round-trip to strip proto zero-value fields (e.g.,
// "sharded": false) that are semantically equivalent to being absent.
// This matches the approach used for VSchema comparison in the tern layer.
func normalizeVSchemaJSON(s string) string {
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
			return string(data)
		}
	}

	// Fallback: JSON round-trip stripping empty maps
	var obj map[string]any
	if err := json.Unmarshal([]byte(s), &obj); err != nil {
		return s
	}
	for key, val := range obj {
		if m, ok := val.(map[string]any); ok && len(m) == 0 {
			delete(obj, key)
		}
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
