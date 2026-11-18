package vschema

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// Elements splits a VSchema document into the parts a change can touch on
// its own: every table keyed "table <name>", every vindex keyed
// "vindex <name>", and everything else the keyspace declares as one element
// keyed "the keyspace settings". Each maps to its canonical JSON, with
// formatting, key order, and proto zero-value fields ignored as in Normalize.
// An empty document is an empty keyspace.
//
// The parse is strict: a document with a field the vendored VSchema
// definition does not know is an error, since dropping the field would hide
// a change to it.
func Elements(content string) (map[string]string, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		content = "{}"
	}
	var ks vschemapb.Keyspace
	if err := protojson.Unmarshal([]byte(content), &ks); err != nil {
		return nil, fmt.Errorf("parse VSchema: %w", err)
	}
	elements := make(map[string]string, len(ks.GetTables())+len(ks.GetVindexes())+1)
	for name, table := range ks.GetTables() {
		definition, err := canonicalJSON(table)
		if err != nil {
			return nil, fmt.Errorf("render table %s: %w", name, err)
		}
		elements["table "+name] = definition
	}
	for name, vindex := range ks.GetVindexes() {
		definition, err := canonicalJSON(vindex)
		if err != nil {
			return nil, fmt.Errorf("render vindex %s: %w", name, err)
		}
		elements["vindex "+name] = definition
	}
	settings := proto.CloneOf(&ks)
	settings.Tables, settings.Vindexes = nil, nil
	definition, err := canonicalJSON(settings)
	if err != nil {
		return nil, fmt.Errorf("render the keyspace settings: %w", err)
	}
	elements["the keyspace settings"] = definition
	return elements, nil
}

// canonicalJSON renders a VSchema message as JSON with proto zero-value
// fields omitted, as Vitess stores it, and object keys sorted.
func canonicalJSON(m proto.Message) (string, error) {
	data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal %s: %w", m.ProtoReflect().Descriptor().Name(), err)
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return "", fmt.Errorf("decode %s: %w", m.ProtoReflect().Descriptor().Name(), err)
	}
	sorted, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("sort %s: %w", m.ProtoReflect().Descriptor().Name(), err)
	}
	return string(sorted), nil
}
